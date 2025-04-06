import praw
import pandas as pd
import logging

# Set up logging
logging.basicConfig(filename='logs/reddit_scraper.log', level=logging.INFO, format='%(asctime)s:%(levelname)s:%(message)s')

reddit_client_id = 'QCBpjYlPaFUIbpKgNFM4_A'
reddit_client_secret = 'I3p3zTh3ThdHARUGnzoNybxYfkom0Q'
reddit_user_agent = 'data_eng'

reddit = praw.Reddit(
    client_id=reddit_client_id,
    client_secret=reddit_client_secret,
    user_agent=reddit_user_agent
)

def scrape_comments(subreddit_name, product_name):
    logging.info(f"Scraping comments for {product_name} in subreddit {subreddit_name}")

    subreddit = reddit.subreddit(subreddit_name)
    hot_posts = subreddit.hot(limit=400)
    controversial_posts = subreddit.controversial(limit=400)

    post_ids = set()

    unique_hot_posts = []
    for post in hot_posts:
        if post.id not in post_ids:
            unique_hot_posts.append(post)
            post_ids.add(post.id)

    unique_controversial_posts = []
    for post in controversial_posts:
        if post.id not in post_ids:
            unique_controversial_posts.append(post)
            post_ids.add(post.id)

    all_posts = unique_hot_posts + unique_controversial_posts

    comment_ids = set()
    comments_list = []

    for submission in all_posts:
        submission.comments.replace_more(limit=500)

        for sort_type in ['top', 'bottom', 'controversial']:
            if sort_type == 'top':
                sorted_comments = sorted(submission.comments, key=lambda comment: comment.score, reverse=True)
            elif sort_type == 'bottom':
                sorted_comments = sorted(submission.comments, key=lambda comment: comment.score)
            else:
                sorted_comments = sorted(submission.comments, key=lambda comment: comment.controversiality, reverse=True)

            for comment in sorted_comments[:1000]:
                if comment.id not in comment_ids:
                    num_replies = len(comment.replies)
                    comment_data = {
                        'Post_Title': submission.title,
                        'Comment_ID': comment.id,
                        'Comment_Author': str(comment.author),
                        'Comment_Body': comment.body,
                        'Comment_Ups': comment.ups,
                        'Comment_Downs': comment.downs,
                        'Comment_Number_of_Replies': num_replies,
                        'Comment_Subreddit': comment.subreddit.display_name,
                        'Comment_Post_ID': submission.id,
                        'Comment_Post_Score': submission.score,
                        'Comment_Post_Number_of_Comments': submission.num_comments,
                        'Comment_Created_Date': pd.to_datetime(comment.created_utc, unit='s'),
                        'Comment_Type': sort_type.capitalize()  # Set comment type
                    }
                    comments_list.append(comment_data)
                    comment_ids.add(comment.id)

    comments_df = pd.DataFrame(comments_list)
    comments_df.to_csv(f"API data/reddit_{product_name}.csv")
    logging.info(f"Finished scraping comments for {product_name}")

# List of product names and corresponding subreddit names
products = [
    {'product_name': 'GalaxyS23', 'subreddit_name': 'GalaxyS23'},
    {'product_name': 'iphone15', 'subreddit_name': 'iphone15'}
]

for product in products:
    scrape_comments(product['subreddit_name'], product['product_name'])

# Merging the CSV files
comments_df_1 = pd.read_csv("API data/reddit_GalaxyS23.csv", index_col=[0])
comments_df_2 = pd.read_csv("API data/reddit_iphone15.csv", index_col=[0])
merged_df = pd.concat([comments_df_1, comments_df_2], ignore_index=True)
merged_df.to_csv("API data/merged_product.csv")
