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
parser.add_argument(
    '-c', '--count', help="How many followers (max: 1000)", default=100
)
parser.add_argument('--pagination-token')

TWEETS_COUNT = 50 # max 200
MINIMUM_TWEET_PER_USER = 10
TWEET_LANG = "fr"

DATA_FOLDER = Path(__file__).parent / "data"
DATA_FOLDER.mkdir(exist_ok=True)


def main(args):
    logger.info("Getting Twitter API client...")
    client = get_api_client(os.getenv("TWITTER_BEARER_TOKEN"))

    logger.info(f"Getting followers of {args.twitter_name}...")
    user_id = get_twitter_id(args.twitter_name, client=client)
    followers, next_token = get_followers(
        user_id,
        client=client,
        count=args.count,
        pagination_token=args.pagination_token
    )
    logger.info(f"Next token for pagination is {next_token}")

    logger.info(f"Getting tweets of followers of {args.twitter_name}...")
    tweets = tweets_from_followers(followers, client=client)

    logger.info(f"Saving {len(tweets)} tweets...")
    save_tweets(tweets, next_token=next_token, account_name=args.twitter_name)


def get_api_client(bearer_token):
    return tweepy.Client(bearer_token)


def get_twitter_id(twitter_name, *, client):
    return client.get_user(username=twitter_name).data.id


def get_followers(user_id, *, client, count, pagination_token):
    resp = client.get_users_followers(
        user_id, max_results=count, pagination_token=pagination_token
    )

    return [user.id for user in resp.data], resp.meta["next_token"]


def tweets_from_followers(followers, *, client):
    def filter_tweet(tweet):
        return tweet.lang == TWEET_LANG

    def filter_user(user_tweets):
        return len(user_tweets) >= MINIMUM_TWEET_PER_USER

    tweets = filter(filter_user, [
        list(filter(filter_tweet, client.get_users_tweets(
            id=follower,
            max_results=TWEETS_COUNT,
            exclude="retweets",
            expansions="author_id",
            tweet_fields="lang"
        ).data or []))
        for follower in followers
    ])

    return list(itertools.chain.from_iterable(tweets))


def get_tweet_data_path(account_name, *, _count=0):
    path = DATA_FOLDER / f"{account_name}-{_count}.csv"
    if path.exists():
        return get_tweet_data_path(account_name, _count=_count + 1)
    return path
    

def save_tweets(tweets, *, next_token, account_name):
    df = pd.DataFrame({
        "tweet": [t.text for t in tweets],
        "user_id": [t.author_id for t in tweets],
        "account": account_name,
        "next_token": next_token
    })

    df.to_csv(get_tweet_data_path(account_name), index=False)


if __name__ == "__main__":
    args = parser.parse_args()
    main(args)
