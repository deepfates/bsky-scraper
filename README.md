# bsky-scraper

This is a Python script that collects posts from the Bluesky firehose and saves them to a JSONL file. This tool is designed to be easy to set up and use, making it accessible for anyone interested in archiving Bluesky posts.

[See the original post about this on Bluesky.](https://bsky.app/profile/deepfates.com.deepfates.com.deepfates.com.deepfates.com.deepfates.com/post/3lbxf5jxqec2e)

## Features

- Collects posts from the Bluesky firehose.
- Saves posts to a JSONL file with details such as text, creation time, author, URI, image presence, and reply information.
- Uses a cache for efficient author handle resolution.

## Requirements

- Python >=3.11,<3.14
- [Poetry](https://python-poetry.org/) for dependency management

## Setup

1. **Clone the repository:**

   ```bash
   git clone https://github.com/deepfates/bsky-scraper.git
   cd bsky-scraper
   ```

2. **Install dependencies:**

   Use Poetry to install the required packages:

   ```bash
   poetry install
   ```

## Usage

1. **Run the script:**

   You can start collecting posts by running the script with Poetry:

   ```bash
   poetry run python scrape.py
   ```

   By default, the script collects posts for 30 seconds. You can adjust the duration by modifying the `duration_seconds` parameter in the `start_collection` method.

2. **Output:**

   The collected posts are saved to `bluesky_posts.jsonl` in the project directory. Each line in the file is a JSON object representing a post.

## Customization

- **Output File:** You can change the output file by passing a different filename to the `FirehoseScraper` constructor.
- **Collection Duration:** Modify the `duration_seconds` parameter in the `start_collection` method to change how long the script collects posts.

## License

This project is licensed under the MIT License. See the [LICENSE](LICENSE) file for details.

## Contributing

Contributions are welcome! Please open an issue or submit a pull request for any improvements or bug fixes.

## Contact

For questions or feedback, please contact [deepfates on Bluesky](https://bsky.app/profile/deepfates.com.deepfates.com.deepfates.com.deepfates.com.deepfates.com).
