import os
import argparse
import random
from pathlib import Path
from pydicom import dcmread
from pydicom.uid import ExplicitVRLittleEndian
from pynetdicom import AE, debug_logger
from pynetdicom.presentation import StoragePresentationContexts
import gc
import time
from datetime import datetime

def send_fold(folder_path, scp_ip, scp_port, ae_title="MY_SCU", scp_ae_title="MY_SCP"):
    """Sends all DICOM files from a folder to the DICOM SCP."""
    ae = AE(ae_title=ae_title)
    ae.dimse_timeout = 600
    ae.network_timeout = 300
    ae.acse_timeout = 120

    stats = {
        'total_files': 0,
        'sent_successfully': 0,
        'failed': 0,
        'reconnections': 0,
        'errors': [],
        'start_time': time.time()
    }
    
    for context in StoragePresentationContexts:
        ae.add_requested_context(context.abstract_syntax, ExplicitVRLittleEndian)
    
    print(f"\n[INFO] Attempting to connect to {scp_ip}:{scp_port}...")
    assoc = ae.associate(scp_ip, scp_port, ae_title=scp_ae_title)
    if not assoc.is_established:
        error_msg = f"[CRITICAL] Initial association with SCP failed at {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}"
        print(error_msg)
        stats['errors'].append(error_msg)
        print_statistics(stats)
        return stats
    
    print(f"[SUCCESS] Association established at {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    
    file_count = 0
    for root, _, files in os.walk(folder_path):
        for file in files:
            if file.lower().endswith(".dcm"):
                stats['total_files'] += 1
                try:
                    if not assoc.is_established:
                        warning_msg = f"[WARNING] Association lost after {file_count} files at {datetime.now().strftime('%H:%M:%S')}. Reconnecting..."
                        print(warning_msg)
                        stats['errors'].append(warning_msg)
                        stats['reconnections'] += 1
                        
                        assoc = ae.associate(scp_ip, scp_port, ae_title=scp_ae_title)
                        if not assoc.is_established:
                            error_msg = f"[CRITICAL] Failed to reconnect after {file_count} files. Stopping."
                            print(error_msg)
                            stats['errors'].append(error_msg)
                            print_statistics(stats)
                            return stats
                        print(f"[SUCCESS] Reconnected successfully at {datetime.now().strftime('%H:%M:%S')}")
                    
                    file_path = os.path.join(root, file)
                    ds = dcmread(file_path)
                    
                    status = assoc.send_c_store(ds)
                    
                    del ds
                    file_count += 1
                    
                    if file_count % 50 == 0:
                        gc.collect()
                        elapsed = time.time() - stats['start_time']
                        rate = file_count / elapsed if elapsed > 0 else 0
                        print(f"[PROGRESS] Processed {file_count}/{stats['total_files']} files | {rate:.1f} files/sec | Memory cleaned")
                    
                    if status:
                        if status.Status == 0x0000:
                            stats['sent_successfully'] += 1
                        else:
                            stats['failed'] += 1
                            error_msg = f"File {file}: Response 0x{status.Status:04X}"
                            if len(stats['errors']) < 10:  # Keep only first 10 errors
                                stats['errors'].append(error_msg)
                    else:
                        stats['failed'] += 1
                        error_msg = f"File {file}: No response from SCP"
                        if len(stats['errors']) < 10:
                            stats['errors'].append(error_msg)
                        
                except Exception as e:
                    stats['failed'] += 1
                    error_msg = f"File {file}: {str(e)}"
                    print(f"[ERROR] {error_msg}")
                    if len(stats['errors']) < 10:
                        stats['errors'].append(error_msg)
    
    stats['end_time'] = time.time()
    if assoc.is_established:
        assoc.release()
        print(f"[INFO] Association released cleanly at {datetime.now().strftime('%H:%M:%S')}")
    gc.collect()
    
    print_statistics(stats)
    return stats


def print_statistics(stats):
    """Print detailed statistics about the transfer."""
    print("\n" + "="*70)
    print("TRANSFER STATISTICS")
    print("="*70)
    
    if 'end_time' in stats:
        duration = stats['end_time'] - stats['start_time']
        print(f"Duration: {duration:.1f} seconds")
        if duration > 0:
            print(f"Transfer rate: {stats['sent_successfully'] / duration:.2f} files/second")
    
    print(f"\nTotal files found: {stats['total_files']}")
    print(f"Successfully sent: {stats['sent_successfully']}")
    print(f"Failed: {stats['failed']}")
    print(f"Reconnections: {stats['reconnections']}")
    
    if stats['total_files'] > 0:
        success_rate = (stats['sent_successfully'] / stats['total_files']) * 100
        print(f"\nSuccess rate: {success_rate:.1f}%")
    
    if stats['errors']:
        print(f"\n⚠️  ERRORS/WARNINGS ({len(stats['errors'])} recorded):")
        for i, error in enumerate(stats['errors'], 1):
            print(f"  {i}. {error}")
        if len(stats['errors']) == 10:
            print("  ... (showing first 10 errors only)")
    
    print("\n" + "-"*70)
    if stats['failed'] == 0 and stats['reconnections'] == 0:
        print("✅ PRODUCTION READY: All files transferred successfully")
    elif stats['failed'] == 0 and stats['reconnections'] > 0:
        print("⚠️  WARNING: Reconnections occurred but all files sent")
    elif stats['failed'] > 0:
        print(f"❌ CRITICAL: {stats['failed']} files failed - investigate before production use")
    print("="*70 + "\n")


def find_patient_dirs(folder_path):
    """Return a sorted list of patient-level directories (depth 1 or 2 under folder_path)."""
    root = Path(folder_path)
    candidates = sorted([p for p in root.iterdir() if p.is_dir()])
    if not candidates:
        return []
    first = candidates[0]
    subdirs = [p for p in first.iterdir() if p.is_dir()]
    if subdirs:
        patient_dirs = []
        for dataset_dir in candidates:
            patient_dirs.extend(sorted([p for p in dataset_dir.iterdir() if p.is_dir()]))
        return patient_dirs
    return candidates


def send_all_dicom_files(folder_path, scp_ip="localhost", scp_port=104, ae_title="MY_SCU", scp_ae_title="MY_SCP", count=None):
    """Send DICOM files from a folder, optionally selecting N random patients."""
    if count is not None:
        patient_dirs = find_patient_dirs(folder_path)
        if not patient_dirs:
            print(f"[ERROR] No patient directories found under {folder_path}")
            return
        selected = random.sample(patient_dirs, min(count, len(patient_dirs)))
        print(f"[INFO] Randomly selected {len(selected)} of {len(patient_dirs)} patients:")
        for p in selected:
            print(f"  {p.name}")
        for patient_dir in selected:
            send_fold(str(patient_dir), scp_ip, scp_port, ae_title, scp_ae_title)
    else:
        send_fold(folder_path, scp_ip, scp_port, ae_title, scp_ae_title)



def callback(ch, method, properties, body):
    print(f"Received: {body.decode()}")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description='Send DICOM files to a DICOM SCP listener')
    parser.add_argument('folder', help='Path to folder containing DICOM files')
    parser.add_argument('--host', default='localhost', help='SCP IP address (default: localhost)')
    parser.add_argument('--port', type=int, default=104, help='SCP port (default: 104)')
    parser.add_argument('--ae-title', default='MY_SCU', help='Application Entity Title of this SCU (default: MY_SCU)')
    parser.add_argument('--scp-ae-title', default='MY_SCP', help='AE Title of the SCP (default: MY_SCP)')
    parser.add_argument('--count', type=int, default=None, help='Number of random patients to send (default: all)')
    parser.add_argument('--debug', action='store_true', help='Enable debug logging')

    args = parser.parse_args()

    if args.debug:
        debug_logger()

    if not os.path.exists(args.folder):
        print(f"[ERROR] Folder does not exist: {args.folder}")
        exit(1)

    print(f"[INFO] Sending DICOM files from: {args.folder}")
    print(f"[INFO] Target SCP: {args.host}:{args.port} (AE Title: {args.scp_ae_title})")

    send_all_dicom_files(args.folder, args.host, args.port, args.ae_title, args.scp_ae_title, count=args.count)