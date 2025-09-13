import requests
import os
import json
from dotenv import load_dotenv

class APIClient:
    def __init__(self, client_id=None, client_secret=None, user_agent=None):
        # Load environment variables from .env file
        load_dotenv()
        
        self.client_id = client_id or os.getenv('REDDIT_CLIENT_ID')
        self.client_secret = client_secret or os.getenv('REDDIT_CLIENT_SECRET')
        self.user_agent = user_agent or os.getenv('REDDIT_USER_AGENT')
        
        if not all([self.client_id, self.client_secret, self.user_agent]):
            raise ValueError("Missing required credentials. Check your .env file or constructor parameters.")
        
        self.access_token = None
        self.base_url = "https://oauth.reddit.com"
        
        # Create data folder in the same directory as the script
        self.data_folder = os.path.join(os.path.dirname(os.path.abspath(__file__)), "data")
        os.makedirs(self.data_folder, exist_ok=True)

    def authenticate(self):
        auth_url = "https://www.reddit.com/api/v1/access_token"
        auth = (self.client_id, self.client_secret)
        headers = {"User-Agent": self.user_agent}
        data = {"grant_type": "client_credentials"}
        
        response = requests.post(auth_url, auth=auth, headers=headers, data=data)
        response.raise_for_status()
        
        self.access_token = response.json()["access_token"]

    def get_subreddit_images(self, subreddit, limit=25, save_json=True):
        if not self.access_token:
            self.authenticate()
        
        headers = {
            "Authorization": f"Bearer {self.access_token}",
            "User-Agent": self.user_agent
        }
        
        url = f"{self.base_url}/r/{subreddit}/hot.json"
        print(f"Fetching data from: {url}")
        params = {"limit": limit}
        
        response = requests.get(url, headers=headers, params=params)
        print(f"Response Status Code: {response}")
        response.raise_for_status()
        
        data = response.json()
        image_posts = []
        image_urls = []
        image_filenames = []
        
        for post in data["data"]["children"]:
            post_data = post["data"]
            if post_data.get("post_hint") == "image" or post_data["url"].endswith(('.jpg', '.jpeg', '.png', '.gif')):
                image_metadata = {
                    'id': post_data["id"],
                    "title": post_data["title"],
                    "author": post_data["author"],
                    "subreddit": post_data["subreddit"],
                    "score": post_data["score"],
                    "url": post_data["url"],
                    "filename": os.path.basename(post_data["url"])
                }

                image_posts.append(image_metadata)
                image_urls.append(post_data["url"])
                image_filenames.append(os.path.basename(post_data["url"]))

        if save_json:
            self.save_to_json(image_posts, subreddit)

        return image_posts, image_urls, image_filenames

    def save_to_json(self, data, subreddit):
        filename = f"{subreddit}_images.json"
        filepath = os.path.join(self.data_folder, filename)
        
        with open(filepath, 'w', encoding='utf-8') as f:
            json.dump(data, f, indent=2, ensure_ascii=False)
        
        print(f"Data saved to: {filepath}")


# Example usage:

if __name__ == "__main__":
    client = APIClient()
    images = client.get_subreddit_images("pics", limit=10)