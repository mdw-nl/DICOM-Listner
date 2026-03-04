import argparse
import json
import sys
import time
from pathlib import Path

import requests

sys.path.insert(0, str(Path(__file__).parent))
from send_dicom import send_all_dicom_files

_MODALITIES = ["RTDOSE", "RTPLAN", "RTSTRUCT"]


def query_rt_package(api_url: str, modality: str) -> dict:
    resp = requests.post(f"{api_url}/rt_package", json={"modality": modality}, timeout=10)
    resp.raise_for_status()
    return resp.json()


def print_packages(result: dict) -> None:
    modality = result["modality"]
    packages = result["packages"]
    print(f"\n{'=' * 60}")
    print(f"  Modality: {modality}  —  {len(packages)} package(s) found")
    print("=" * 60)
    if not packages:
        print("  (none)")
        return
    for i, pkg in enumerate(packages, 1):
        print(f"\n  Package {i}:")
        for key, val in pkg.items():
            print(f"    {key:<22} {val}")


def summarize(results: list[dict]) -> None:
    print(f"\n{'=' * 60}")
    print("  SUMMARY")
    print("=" * 60)
    for result in results:
        modality = result["modality"]
        count = len(result["packages"])
        patients = {p["patient_id"] for p in result["packages"]}
        print(f"  {modality:<10} {count:>3} package(s)  |  patients: {sorted(patients) or '—'}")
    print("=" * 60)


def main() -> None:
    parser = argparse.ArgumentParser(description="Send DICOM data and test /rt_package endpoint")
    parser.add_argument("--data-path", default=str(Path.home() / "data"), help="Root data folder (default: ~/data)")
    parser.add_argument("--patients", type=int, default=10, help="Number of patients to send (default: 10)")
    parser.add_argument(
        "--api-url", default="http://localhost:9000", help="API base URL (default: http://localhost:9000)"
    )
    parser.add_argument("--scp-host", default="localhost", help="DICOM SCP host (default: localhost)")
    parser.add_argument("--scp-port", type=int, default=104, help="DICOM SCP port (default: 104)")
    parser.add_argument(
        "--wait", type=int, default=30, help="Seconds to wait for processing after sending (default: 30)"
    )
    parser.add_argument("--skip-send", action="store_true", help="Skip sending; only query the API")
    args = parser.parse_args()

    if not args.skip_send:
        print(f"[INFO] Sending {args.patients} patient(s) from {args.data_path} → {args.scp_host}:{args.scp_port}")
        send_all_dicom_files(
            folder_path=args.data_path,
            scp_ip=args.scp_host,
            scp_port=args.scp_port,
            count=args.patients,
        )
        print(f"\n[INFO] Waiting {args.wait}s for background processing...")
        time.sleep(args.wait)

    print(f"\n[INFO] Querying {args.api_url}/rt_package for all modalities...")
    results = []
    for modality in _MODALITIES:
        try:
            result = query_rt_package(args.api_url, modality)
            results.append(result)
            print_packages(result)
        except requests.HTTPError as e:
            print(f"\n[ERROR] {modality}: HTTP {e.response.status_code} — {e.response.text}")
        except requests.ConnectionError:
            print(f"\n[ERROR] Could not connect to {args.api_url}. Is the API running?")
            return

    if results:
        summarize(results)

    print("\n[INFO] Full JSON responses saved to rt_package_results.json")
    Path("rt_package_results.json").write_text(json.dumps(results, indent=2))


if __name__ == "__main__":
    main()
