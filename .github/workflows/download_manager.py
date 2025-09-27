import requests
from concurrent.futures import ThreadPoolExecutor
import os
from tqdm import tqdm

class DownloadManager:
    def __init__(self, url, output_file, num_segments=4):
        self.url = url
        self.output_file = output_file
        self.num_segments = num_segments

    def get_file_size(self):
        response = requests.head(self.url)
        return int(response.headers.get("content-length", 0))

    def download_segment(self, start, end, segment_index):
        headers = {"Range": f"bytes={start}-{end}"}
        response = requests.get(self.url, headers=headers, stream=True)
        segment_file = f"{self.output_file}.part{segment_index}"
        with open(segment_file, "wb") as f:
            for chunk in response.iter_content(chunk_size=1024):
                if chunk:
                    f.write(chunk)
        return segment_file

    def combine_segments(self, segment_files):
        with open(self.output_file, "wb") as f:
            for segment in segment_files:
                with open(segment, "rb") as sf:
                    f.write(sf.read())
                os.remove(segment)

    def download(self):
        file_size = self.get_file_size()
        segment_size = file_size // self.num_segments

        segment_files = []
        with ThreadPoolExecutor(max_workers=self.num_segments) as executor:
            futures = []
            for i in range(self.num_segments):
                start = i * segment_size
                end = start + segment_size - 1 if i < self.num_segments - 1 else file_size
                futures.append(executor.submit(self.download_segment, start, end, i))

            for future in tqdm(futures, desc="Downloading", unit="segment"):
                segment_files.append(future.result())

        self.combine_segments(segment_files)

if __name__ == "__main__":
    import argparse
    parser = argparse.ArgumentParser(description="Simple Download Manager")
    parser.add_argument("url", help="File URL to download")
    parser.add_argument("-o", "--output", default="output.file", help="Output file name")
    parser.add_argument("-s", "--segments", type=int, default=4, help="Number of segments")
    args = parser.parse_args()

    dm = DownloadManager(args.url, args.output, args.segments)
    dm.download()
