import os
import re
from src.shared.logger import setup_logger

logger = setup_logger(__name__)

def to_kebab_case(filename: str) -> str:

    name, ext = os.path.splitext(filename)
    kebab_name = re.sub(r"[\s_/.]+", "-", name.lower())
    kebab_name = re.sub(r"-+", "-", kebab_name)
    kebab_name = kebab_name.strip("-")
    return f"{kebab_name}{ext.lower()}"

if __name__ == "__main__":
    # Test cases

    try:

        test_filenames = [
                "adas   adasdaere /// adasd.jpg",
                "My Image_File.PNG",
        ]
    
        for filename in test_filenames:
            logger.info(f"Original: {filename} --> Kebab-case: {to_kebab_case(filename)}")

    except Exception as e:
        logger.error(f"Error during kebab-case conversion tests: {e}")