#!/usr/bin/env python3
"""
Task 1 - Repository Cloning and Python File Discovery.

Chức năng:
1. Shallow-clone repository huggingface/datasets.
2. Tìm tất cả file .py.
3. Lọc test, setup và generated files.
4. Tạo manifest để Task 2 xử lý từng file.
5. Ghi lại repository URL, branch, commit SHA và số lượng file.
"""

from __future__ import annotations

import argparse
import hashlib
import json
import shutil
import subprocess
import sys
from datetime import datetime, timezone
from pathlib import Path
from typing import Any


DEFAULT_REPO_URL = "https://github.com/huggingface/datasets.git"
DEFAULT_REPOSITORY_NAME = "huggingface/datasets"

# Những thư mục không đưa vào danh sách Parser Service.
EXCLUDED_DIRECTORIES = {
    ".git",
    ".github",
    ".venv",
    "venv",
    "__pycache__",
    "tests",
    "test",
    "build",
    "dist",
}

# Setup files được đề bài cho phép loại.
EXCLUDED_FILENAMES = {
    "setup.py",
}

# Một số file thường được công cụ tự động sinh ra.
GENERATED_SUFFIXES = (
    "_pb2.py",
    "_pb2_grpc.py",
)


def run_command(
    command: list[str],
    cwd: Path | None = None,
) -> str:
    """Chạy command và trả về stdout."""

    try:
        result = subprocess.run(
            command,
            cwd=cwd,
            check=True,
            text=True,
            capture_output=True,
        )
    except FileNotFoundError as error:
        raise RuntimeError(
            f"Không tìm thấy chương trình: {command[0]}"
        ) from error
    except subprocess.CalledProcessError as error:
        stderr = error.stderr.strip() if error.stderr else "Không có stderr"
        raise RuntimeError(
            f"Lệnh thất bại: {' '.join(command)}\n{stderr}"
        ) from error

    return result.stdout.strip()


def clone_repository(
    repo_url: str,
    clone_directory: Path,
    refresh: bool,
) -> None:
    """Shallow-clone repository nếu chưa tồn tại."""

    if refresh and clone_directory.exists():
        print(f"[INFO] Xóa repository cũ: {clone_directory}")
        shutil.rmtree(clone_directory)

    if clone_directory.exists():
        git_directory = clone_directory / ".git"

        if not git_directory.exists():
            raise RuntimeError(
                f"{clone_directory} đã tồn tại nhưng không phải Git repository."
            )

        print(f"[INFO] Sử dụng repository đã có: {clone_directory}")
        return

    clone_directory.parent.mkdir(parents=True, exist_ok=True)

    print(f"[INFO] Shallow-clone {repo_url}")

    run_command(
        [
            "git",
            "clone",
            "--depth",
            "1",
            "--single-branch",
            repo_url,
            str(clone_directory),
        ]
    )


def sha256_bytes(data: bytes) -> str:
    """Tạo SHA-256 từ bytes."""

    return hashlib.sha256(data).hexdigest()


def get_exclusion_reason(relative_path: Path) -> str | None:
    """Trả về lý do loại file hoặc None nếu file được chọn."""

    path_parts = set(relative_path.parts)

    excluded_dirs = path_parts.intersection(EXCLUDED_DIRECTORIES)
    if excluded_dirs:
        directory = sorted(excluded_dirs)[0]
        return f"excluded_directory:{directory}"

    if relative_path.name in EXCLUDED_FILENAMES:
        return f"excluded_filename:{relative_path.name}"

    if relative_path.name.endswith(GENERATED_SUFFIXES):
        return "generated_file"

    return None


def count_lines(data: bytes) -> int:
    """Đếm số dòng, kể cả dòng cuối không kết thúc bằng newline."""

    if not data:
        return 0

    line_count = data.count(b"\n")

    if not data.endswith(b"\n"):
        line_count += 1

    return line_count


def write_text_lines(path: Path, values: list[str]) -> None:
    """Ghi mỗi giá trị trên một dòng."""

    content = "\n".join(values)

    if values:
        content += "\n"

    path.write_text(content, encoding="utf-8")


def write_json(path: Path, value: Any) -> None:
    """Ghi JSON có format dễ đọc."""

    path.write_text(
        json.dumps(
            value,
            ensure_ascii=False,
            indent=2,
            sort_keys=True,
        )
        + "\n",
        encoding="utf-8",
    )


def write_json_lines(path: Path, values: list[dict[str, Any]]) -> None:
    """Ghi JSON Lines: một JSON object trên mỗi dòng."""

    with path.open("w", encoding="utf-8", newline="\n") as output_file:
        for value in values:
            output_file.write(
                json.dumps(
                    value,
                    ensure_ascii=False,
                    sort_keys=True,
                )
            )
            output_file.write("\n")


def discover_python_files(
    clone_directory: Path,
    repository_name: str,
) -> tuple[list[str], list[dict[str, Any]], list[dict[str, Any]]]:
    """
    Tìm file Python và tạo manifest.

    Returns:
        all_files:
            Danh sách toàn bộ file .py.

        selected_manifest:
            Metadata của những file được chọn.

        excluded_manifest:
            File bị loại kèm lý do.
    """

    all_paths = sorted(
        path
        for path in clone_directory.rglob("*.py")
        if path.is_file()
    )

    all_files: list[str] = []
    selected_manifest: list[dict[str, Any]] = []
    excluded_manifest: list[dict[str, Any]] = []

    for absolute_path in all_paths:
        relative_path = absolute_path.relative_to(clone_directory)
        relative_path_string = relative_path.as_posix()

        all_files.append(relative_path_string)

        exclusion_reason = get_exclusion_reason(relative_path)

        if exclusion_reason is not None:
            excluded_manifest.append(
                {
                    "path": relative_path_string,
                    "reason": exclusion_reason,
                }
            )
            continue

        file_bytes = absolute_path.read_bytes()

        # file_id không phụ thuộc đường dẫn tuyệt đối trên máy của thành viên.
        file_id_source = (
            f"{repository_name}:{relative_path_string}"
        ).encode("utf-8")

        selected_manifest.append(
            {
                "file_id": sha256_bytes(file_id_source),
                "path": relative_path_string,
                "language": "python",
                "size_bytes": len(file_bytes),
                "line_count": count_lines(file_bytes),
                "content_sha256": sha256_bytes(file_bytes),
            }
        )

    return all_files, selected_manifest, excluded_manifest


def parse_arguments() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description=(
            "Shallow-clone repository và tìm/lọc các file Python."
        )
    )

    parser.add_argument(
        "--repo-url",
        default=DEFAULT_REPO_URL,
        help="URL của repository cần phân tích.",
    )

    parser.add_argument(
        "--repository-name",
        default=DEFAULT_REPOSITORY_NAME,
        help="Tên ổn định dùng để tạo file_id.",
    )

    parser.add_argument(
        "--clone-dir",
        type=Path,
        default=Path(".work/repos/datasets"),
        help="Thư mục clone repository nguồn.",
    )

    parser.add_argument(
        "--output-dir",
        type=Path,
        default=Path("artifacts/task1"),
        help="Thư mục chứa kết quả Task 1.",
    )

    parser.add_argument(
        "--refresh",
        action="store_true",
        help="Xóa clone cũ và clone lại repository.",
    )

    return parser.parse_args()


def main() -> int:
    args = parse_arguments()

    clone_directory: Path = args.clone_dir.resolve()
    output_directory: Path = args.output_dir.resolve()

    try:
        clone_repository(
            repo_url=args.repo_url,
            clone_directory=clone_directory,
            refresh=args.refresh,
        )

        commit_sha = run_command(
            ["git", "rev-parse", "HEAD"],
            cwd=clone_directory,
        )

        branch = run_command(
            ["git", "branch", "--show-current"],
            cwd=clone_directory,
        )

        is_shallow = run_command(
            ["git", "rev-parse", "--is-shallow-repository"],
            cwd=clone_directory,
        )

        all_files, selected_manifest, excluded_manifest = (
            discover_python_files(
                clone_directory=clone_directory,
                repository_name=args.repository_name,
            )
        )

        selected_files = [
            item["path"] for item in selected_manifest
        ]

        output_directory.mkdir(parents=True, exist_ok=True)

        repository_info = {
            "repository": args.repository_name,
            "repository_url": args.repo_url,
            "branch": branch,
            "commit_sha": commit_sha,
            "is_shallow_repository": is_shallow == "true",
            "discovered_at": datetime.now(timezone.utc).isoformat(),
        }

        summary = {
            **repository_info,
            "all_python_files": len(all_files),
            "selected_python_files": len(selected_manifest),
            "excluded_python_files": len(excluded_manifest),
            "filter_policy": {
                "excluded_directories": sorted(EXCLUDED_DIRECTORIES),
                "excluded_filenames": sorted(EXCLUDED_FILENAMES),
                "generated_suffixes": list(GENERATED_SUFFIXES),
            },
        }

        write_json(
            output_directory / "repository_info.json",
            repository_info,
        )

        write_json(
            output_directory / "summary.json",
            summary,
        )

        write_text_lines(
            output_directory / "python_files_all.txt",
            all_files,
        )

        write_text_lines(
            output_directory / "python_files_selected.txt",
            selected_files,
        )

        write_json_lines(
            output_directory / "python_files_excluded.jsonl",
            excluded_manifest,
        )

        write_json_lines(
            output_directory / "python_manifest.jsonl",
            selected_manifest,
        )

        print("\n========== TASK 1 SUMMARY ==========")
        print(f"Repository       : {args.repository_name}")
        print(f"Branch           : {branch or '(detached HEAD)'}")
        print(f"Commit SHA       : {commit_sha}")
        print(f"Shallow clone    : {is_shallow}")
        print(f"All Python files : {len(all_files)}")
        print(f"Selected files   : {len(selected_manifest)}")
        print(f"Excluded files   : {len(excluded_manifest)}")
        print(f"Output directory : {output_directory}")
        print("====================================")

        return 0

    except RuntimeError as error:
        print(f"[ERROR] {error}", file=sys.stderr)
        return 1


if __name__ == "__main__":
    raise SystemExit(main())