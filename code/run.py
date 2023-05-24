import sys
from utils_slack import *

parallelize = True if sys.argv[1].lower() == 'parallel' or sys.argv[1].lower() == 'p' else False
data_dir = sys.argv[2]
seed_val = sys.argv[3]
os.chdir(data_dir)

process_workspace(path_liwc_dict = 'liwc2007dictionary_poster.csv',
                  channels_dir = data_dir, 
                  list_rem_gen = ['text', 'reactions', 'type', 'user_team', 'source_team', 'user_profile', 'attachments', 'files', 'upload', 'display_as_bot', 'edited', 'thread_ts'],
                  list_rem_thread = ['reply_count', 'reply_users_count', 'latest_reply','is_locked', 'subscribed', 'last_read', 'thread_ts', 'reply_users'], 
                  list_rem_blk = ['type', 'block_id'], 
                  print_cond = False, 
                  seed_val = seed_val,
                  parallel = parallelize)

# Generate dictionary of user IDs and hashed email IDs (If applicable)
#hash_ids('members.csv')