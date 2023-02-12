import argparse
import itertools
import logging
import os
from pathlib import Path

from dotenv import load_dotenv
import pandas as pd
import tweepy


load_dotenv()

logging.basicConfig(
    level=logging.INFO,
    format="[%(asctime)s] %(levelname)s - %(message)s"
)
logger = logging.getLogger(__name__)

parser = argparse.ArgumentParser()
parser.add_argument('twitter_name', help="""
    Twitter username of the account to extract.
""")

FOLLOWERS_COUNT = 1000 # max 1000
TWEETS_COUNT = 50 # max 200
MINIMUM_TWEET_PER_USER = 10

DATA_FOLDER = Path(__file__).parent / "data"
DATA_FOLDER.mkdir(exist_ok=True)


def main(args):
    logger.info("Getting Twitter API client...")
    client = get_api_client(os.getenv("TWITTER_BEARER_TOKEN"))

    logger.info(f"Getting followers of {args.twitter_name}...")
    user_id = get_twitter_id(args.twitter_name, client=client)
    followers = get_followers(user_id, client=client)

    logger.info(f"Getting tweets of followers of {args.twitter_name}...")
    tweets = tweets_from_followers(followers, client=client)

    logger.info(f"Saving {len(tweets)} tweets...")
    save_tweets(tweets, account_name=args.twitter_name)


def get_api_client(bearer_token):
    return tweepy.Client(bearer_token)


def get_twitter_id(twitter_name, *, client):
    return client.get_user(username=twitter_name).data.id


def get_followers(user_id, *, client):
    return [
        user.id
        for user
        in client.get_users_followers(user_id, max_results=FOLLOWERS_COUNT).data
    ]


def tweets_from_followers(followers, *, client):
    def filter_and_flatten(l):
        return list(itertools.chain.from_iterable(
            filter(
                lambda e: e is not None and len(e) >= MINIMUM_TWEET_PER_USER, l
            )
        ))

    return filter_and_flatten([
        client.get_users_tweets(
            id=follower,
            max_results=TWEETS_COUNT,
            exclude="retweets",
            expansions="author_id"
        ).data
        for follower in followers
    ])


def save_tweets(tweets, *, account_name):
    df = pd.DataFrame({
        "tweet": [t.text for t in tweets],
        "user_id": [t.author_id for t in tweets],
        "account": account_name
    })
    df.to_csv(f"{DATA_FOLDER / account_name}.csv", index=False)


if __name__ == "__main__":
    args = parser.parse_args()
    main(args)
