# -*- coding: utf-8 -*-

import contractions
import pandas as pd
import numpy as np
import string
import json
import os
import hashlib
import re
from datetime import datetime
import multiprocessing
import nltk
from nltk.stem import *
import csv
from collections import Counter
from operator import add, itemgetter
import shutil
stemmer = PorterStemmer()



def rem_items(msg_json, list_rem_gen, list_rem_thread, print_cond):
  """
  rem_items removes unnecessary keys in the message JSON 

  :param msg_json: JSON contents of the message
  :param list_rem_gen: list of general keys to be removed
  :param list_rem_thread: list of keys to be removed that relate to threading or responses
  :print_cond: "True" or "False" to select whether errors or warnings should be printed
  """ 

  # Generating an ID for the message 
  if 'client_msg_id' in msg_json.keys():
    id = str(msg_json['client_msg_id'])
  else:
    id = 'NO CLIENT MESSAGE ID AVAILABLE'

  # Removing general items
  for key in list_rem_gen:
    try:
      msg_json.pop(key)
    except:
      if print_cond:
        print(str(key) + ' is not present in ' + id + ' message')
  
  # Removing items that apply to messages with responses (thread)
  if 'reply_count' in msg_json.keys():
    if print_cond:
      print(id + ' message contains responses (thread)')
    for key in list_rem_thread:
      try:
        msg_json.pop(key)
      except:
        if print_cond:
          print(str(key) + ' is not present in ' + id + ' message')

  # Convert UNIX time stamp of message to month, day, year, hour, minute, second format
  convert_unix_ts(msg_json)
  


def rem_blk_items(msg_json, list_rem_blk, print_cond):
  """
  rem_blk_items removes unecessary contents under the "blocks" key of the message JSON

  :param msg_json: JSON contents of the message
  :param list_rem_blk: list of keys under "blocks" to be removed
  :print_cond: "True" or "False" to select whether errors or warnings should be printed
  """ 

  # Generating an ID for the message 
  if 'client_msg_id' in msg_json.keys():
    id = str(msg_json['client_msg_id'])
  else:
    id = 'NO CLIENT MESSAGE ID AVAILABLE'

  # Removing items under block item 
  for key in list_rem_blk:
    try: 
      msg_json['blocks'][0].pop(key)
    except:
      if print_cond:
        print(str(key) + ' is not present in ' + id + ' message')

  

def convert_unix_ts(msg_json):
  """
  convert_unix_ts changes the format of all time stamps in the message JSON from unix to date-time

  :param msg_json: JSON contents of the message
  """ 

  # Generate time stamp for main time stamp key
  msg_json['ts'] = datetime.utcfromtimestamp(int(float(msg_json['ts']))).strftime('%Y-%m-%d %H:%M:%S')
  if 'replies' in msg_json.keys():
    # Generate time stamps for all replies 
    for resp in msg_json['replies']:
      resp['ts'] = datetime.utcfromtimestamp(int(float(resp['ts']))).strftime('%Y-%m-%d %H:%M:%S')



def add_channel_name(msg_json, channel_name):
  """
  add_channel_name includes 'channel': 'name of channel' as a key-value pair in the message JSON

  :param msg_json: JSON contents of the message
  :param channel_name: name of the Slack channel the message belongs to
  """ 

  # Add channel as a key to the message
  msg_json['channel'] = channel_name



def extract_blk_elements(msg_json):
  """
  extract_blk_elements flattens all nested text contents under the "blocks" key of the message JSON and re-assigns it to the "blocks" key

  :param msg_json: JSON contents of the message
  """ 

  elements_list = []
  for blk in msg_json['blocks'][0]['elements']:
    
    # Main block is a section
    if blk['type'] in ['rich_text_section', 'rich_text_preformatted', 'rich_text', 'rich_text_quote']:
      elements_list.extend(blk['elements'])
    
    # Main block is a list of multiple blocks
    elif blk['type'] == 'rich_text_list':
      for sub_blk in blk['elements']:
        elements_list.extend(sub_blk['elements'])
    
    # Main block is something else
    else:
      print(blk['type'] + ' is not recognized and elements cannot be extracted')
  
  # Assigning formatted and aggregated items to the blocks item
  msg_json['blocks'] = elements_list



def clean_text(text): 
  """
  clean_text removes unnecessary characters, punctuations in text content  

  :param text: any string
  :return: modified string
  """ 

  # Replacing certain sets of characters with spaces 
  text = text.replace('\xa0', ' ')
  text = text.replace('\r', ' ')
  text = text.replace('\t', ' ')
  
  # Lowercase all text
  text = text.lower()

  # Expanding contractions or abbreviations
  for word in text.split():
    # Addressing contractions like "it's", "hasn't", etc. using module
    text = text.replace(word, (contractions.fix(word)))
    # Lowercase the text again since replacement with contractions module may reverse the initial lowercasing
    text = text.lower()

    # Addressing common abbreviations
    if 'e.g' in word:
      text = text.replace('e.g', 'example')
    if 'vs' in word:
     text = text.replace('vs', 'versus')
    if 'i.e' in word:
      text = text.replace('i.e', 'that is')
  
  text = re.sub(r'[0-9]+', ' NUM ', text)
  text = re.sub(r'[\?|!|\.]+', ' SENTEND ', text)
  text = re.sub(r'é|è', 'e', text)
  text = re.sub(r'[^A-Za-z]+', ' ', text)
    
  text = text.replace('SENTEND', 'SENT_END')
  
  return text



def clean_repeats_in_text(text):
  """
  clean_repeats_in_text removes any repititions of NUM, SENT_END or newline characters 

  :param text: any string
  :return: modified string
  """ 
  # General cleaning
  text = re.sub('\s+', ' ', text) 
  text = text.replace('NUM th', 'NUM')
  text = text.replace('NUM st', 'NUM')
  text = text.replace('NUM nd', 'NUM')

  # Replacing repetitions of "NUM" or "\n" of any number, and separating by any number of spaces, with a single "NUM" or "\n"
  text = re.sub('(NUM *)+', ' NUM ', text) 
  text = re.sub('(SENT_END *)+', ' SENT_END ', text) 
  text = re.sub('(\n *)+', ' \n ', text) 
  
  # Replacing more than one space anywhere in text with a single space
  text = re.sub('\s+', ' ', text) 
  text = text.strip()
  
  return text

    

def combine_blk_content(msg_json):
  """
  combine_blk_content combine all relevant contents under the "blocks" structure of the message JSON and assign it to a new key "all content"

  :param msg_json: JSON contents of the message
  """ 

  # Initiliazing empty string for all message contents
  all_msg_content = ''  

  for elm in msg_json['blocks']:
    # Initiliazing empty string for content per element
    elm_content = ''

    # Formatting text items
    if elm['type'] == 'text':
      elm_content = clean_text(elm['text'])
    
    # Addressing links 
    elif elm['type'] == 'link':
    # Capturing text associated with a link if the item exists
      if 'text' in elm.keys():
        link_text = elm['text']
        # Sometimes the text associated with a link is the link itself: "www.abc.com" is present under the text and hyperlink items in the block
        # Create exception to avoid capturing the text in these such instances
        if ('www' not in link_text) and ('.com' not in link_text):
          # Add () around LINK for hyperlinked text
          elm_content = clean_text(link_text)+' (LINK)'
        else: 
          elm_content = 'LINK'
      else:
        elm_content = 'LINK'
   
    # Addressing emojis
    elif elm['type'] == 'emoji':
      elm_content = elm['name']+'EMOJI'
    
    # Addressing user IDs
    elif elm['type'] == 'user':
      elm_content = '@'+elm['user_id']+'USERID'

    # Addressing channel broadcast
    elif elm['type'] == 'broadcast':
      elm_content = '@'+elm['range']+'USERID'

    # Combining all text elements into a single string
    all_msg_content += (' ' + elm_content)
    all_msg_content = clean_repeats_in_text(all_msg_content)
  
  # Creating a key for all content in the main json
  msg_json['all content'] = all_msg_content

  # Removing blocks key since all content has been combined
  msg_json.pop('blocks')



def check_val_in_list(val, list_words):
  """
  check_val_in_list checks if a given value is present in any word in a list of words

  :param val: string
  :param list_words: list of strings
  :return: True or False
  """ 

  # Checking if a given value is present in any word in a list of words
  for word in list_words:
    if word in val:
      return True 
  return False



def hash_all_text(msg_json, universal_hash_dict, seed_val):
  """
  hash_all_text use md5 to hash the text content and store it under a new "hashed content" key in the message JSON

  :param msg_json: JSON contents of the message
  :param universal_hash_dict: dictionary of hashes and tokens (continuously updated for each message)
  :param seed_val: parameter added to every token before hashing
  :return universal_hash_dict: dictionary of hashes and tokens
  """ 
  all_hashes_str = ''
  
  for word in msg_json['all content'].split():
    
    # Not hashing links, emojis, numbers or user IDs
    if not check_val_in_list(word, ['LINK', 'EMOJI', 'NUM', 'SENT_END', 'USERID']):
      # Adding seed value to the word to be hashed
      hash_object = hashlib.md5((word+str(seed_val)).encode())
      # Hexadecimal version of the md5hash
      repl = str(hash_object.hexdigest())[0:8]
      
      # Adding words and hashes to a universal hash dictionary
      if word not in universal_hash_dict.keys():
        universal_hash_dict[word] = repl

    # If the word is one of the not-to-be hashed elements, the word itself is used in place of a hash
    elif check_val_in_list(word, ['LINK', '(LINK)', 'EMOJI', 'NUM', 'SENT_END', 'USERID']):
      repl = word.replace('USERID', '').replace('EMOJI', '')

    # Combining all the hashes
    all_hashes_str += (repl + ' ')
  
  # Add list of hashes to the message
  msg_json['hashed content'] = all_hashes_str.strip()
  # Remove identifiers from all content since hashing is done
  msg_json['all content'] = msg_json['all content'].replace('USERID', '').replace('EMOJI', '')

  return universal_hash_dict



def mod_msg_jsons_in_list(msgs_list, list_rem_gen, list_rem_thread, list_rem_blk, print_cond, 
                          universal_hash_dict, seed_val, channel_name):
  """
  mod_msg_jsons_in_list calls a sequence of functions to process every message JSON in a list of messages

  :param msgs_list: list of message JSONs
  :param list_rem_gen: list of general keys to be removed from each message JSON
  :param list_rem_thread: list of keys to be removed from each message JSON that relate to threading or responses
  :param list_rem_blk: list of keys under "blocks" to be removed
  :param print_cond: "True" or "False" to select whether errors or warnings should be printed
  :param universal_hash_dict: dictionary of hashes and tokens (continuously updated for each message)
  :param seed_val: parameter added to every token before hashing
  :param channel_name: name of the Slack channel the message belongs to
  :return: list of modified messages and list of unprocessed messages
  """ 
  
  # Create a list of selected messages
  msgs_selected = [msg_json for msg_json in msgs_list if 'client_msg_id' in msg_json.keys()]
  # Create a list of unselected messages
  msgs_not_selected = [msg_json for msg_json in msgs_list if msg_json not in msgs_selected]

  # Processing messages in the selected group of messages
  for msg_json in msgs_selected:
    add_channel_name(msg_json, channel_name)
    rem_items(msg_json, list_rem_gen, list_rem_thread, print_cond)
    if 'blocks' in msg_json.keys():
      rem_blk_items(msg_json, list_rem_blk, print_cond)
      extract_blk_elements(msg_json)
      combine_blk_content(msg_json)
      universal_hash_dict = hash_all_text(msg_json, universal_hash_dict, seed_val)
      msg_json['LIWC dict'] = body_to_liwc(msg_json['all content'])
      msg_json.pop('all content')
  return msgs_selected, msgs_not_selected

def process_channel(path_liwc_dict, channels_dir, channel_name, output_dir, list_rem_gen, list_rem_thread, list_rem_blk, print_cond, seed_val):
  """
  process_workspace_channel loads necessary files, calls main function for message JSON processing, and writes results to specified path
  :param path_liwc_dict: name of .csv file that contains the LIWC dictionary
  :param channels_dir: path to directory that contains the exported Slack workspace (channel folders, user.json, logs.json, etc.)
  :param channel_name: name of the Slack channel the message belongs to
  :param output_dir: directory of the output of data
  :param list_rem_gen: list of general keys to be removed from each message JSON
  :param list_rem_thread: list of keys to be removed from each message JSON that relate to threading or responses
  :param print_cond: "True" or "False" to select whether errors or warnings should be printed
  :param seed_val: parameter added to every token before hashing
  """ 

  print('Processing channel {} at: {}'.format(channel_name, datetime.now().strftime("%Y-%m-%d %H:%M:%S")))
      
  # Initialize dictionary with tokens and hashes
  hash_dict = {}
  
  # New channel name with "mod"
  channel_mod = os.path.join(output_dir, channel_name)
  if not os.path.exists(channel_mod):
    os.mkdir(channel_mod)

  # Initializing a list for messages that do not get processed
  all_msgs_not_processed = []

  # Extracting messages from every day (each file) in the channel
  for day in os.listdir(channel_name):
    with open(os.path.join(channel_name, day), 'r', encoding = 'utf-8') as f:
      msgs_per_day = json.load(f)

    # Call the function to process messages in a list
    mod_msgs_per_day, msgs_not_processed_per_day = mod_msg_jsons_in_list(msgs_per_day, list_rem_gen, list_rem_thread, list_rem_blk, 
                                                                          print_cond, hash_dict, seed_val, channel_name)

    # Creating a list of messages do not get processed across all days
    all_msgs_not_processed.extend(msgs_not_processed_per_day)

    # Upload modified messages to new folder in output directory
    with open(os.path.join(channel_mod, day), 'w', encoding = 'utf-8') as f:
      json.dump(mod_msgs_per_day, f, ensure_ascii = False, indent = 4)

    # Add list of unprocessed messages in channel to the new "channelname_mod" folder
    with open(os.path.join(channels_dir, channel_name, 'messages_not_processed.json'), 'w', encoding = 'utf-8') as f:
      json.dump(all_msgs_not_processed, f, ensure_ascii = False, indent = 4)
  return hash_dict

def process_workspace(path_liwc_dict, channels_dir, list_rem_gen, list_rem_thread, list_rem_blk, print_cond, seed_val, parallel):
  """
  process_workspace loads necessary files, loops through all workspace channels, processes message JSONs, and writes results to specified path

  :param path_liwc_dict: name of .csv file that contains the LIWC dictionary
  :param channels_dir: path to directory that contains the exported Slack workspace (channel folders, user.json, logs.json, etc.)
  :param list_rem_gen: list of general keys to be removed from each message JSON
  :param list_rem_thread: list of keys to be removed from each message JSON that relate to threading or responses
  :param list_rem_blk: list of keys under "blocks" to be removed
  :param print_cond: True or False to select whether errors or warnings should be printed
  :param seed_val: parameter added to every token before hashing
  :param parallel: whether channels should be processed sequentially or in parallel
  """ 
  start = datetime.now()
  print("Processing started at: ", start.strftime("%Y-%m-%d %H:%M:%S"))
  
  # Output_dir is in the same directory as the data
  output_dir = os.path.join(os.path.abspath(os.path.join(channels_dir, os.pardir)), 'slack_output')
  # Removing directory if it already exists so we start fresh 
  if os.path.exists(output_dir):
    shutil.rmtree(output_dir)
  os.mkdir(output_dir)
  
  # Initialize dictionary with tokens and hashes
  hash_dict = {}
  # Read LIWC dictionary
  read_liwc_dictionary(path_liwc_dict)

  if parallel:
    pool = multiprocessing.Pool(processes = multiprocessing.cpu_count())
    results = [pool.apply_async(process_channel, args = (path_liwc_dict, channels_dir, channel_name, output_dir, list_rem_gen, list_rem_thread, list_rem_blk, print_cond, seed_val)) 
              for channel_name in os.listdir(channels_dir) if (os.path.isdir(channel_name))]
    pool.close()
    pool.join()
    # Combining all results
    for r in results:
      hash_dict.update(r.get())
  
  else:
    # Extracting all channels in the Slack workspace
    channel_list = [channel_name for channel_name in os.listdir(channels_dir) if (os.path.isdir(channel_name))]
    list_hash_dict = []
    # Process messages in each channel
    for channel_name in channel_list:
      curr_hash_dict = process_channel(path_liwc_dict, channels_dir, channel_name, output_dir, list_rem_gen, list_rem_thread, list_rem_blk, print_cond, seed_val)
      list_hash_dict.append(curr_hash_dict)
    
    # Combining hash dictionaries across all channels
    for h in list_hash_dict:
      hash_dict.update(h)
      
  # Upload hash dictionary (across all channels) to the directory    
  with open(os.path.join(output_dir, 'hash_dict.json'), 'w', encoding = 'utf-8') as f:
    json.dump(hash_dict, f, ensure_ascii = False, indent = 4)

  end = datetime.now()
  print("Processing ended at: ", end.strftime("%Y-%m-%d %H:%M:%S"))
  print("Total processing time: ", end - start)

def hash_ids(csv_path):
  """
  hash_ids takes a .csv file with metadata of workspace users and returns a dict of user IDs and hashed email IDs

  :param csv_path: path of the .csv file
  """
  # Read user details export
  df = pd.read_csv(csv_path)
  # Initialize dictionary of hashes and tokens
  hash_dict_ids = {}
  # Extract info per user
  for row in df.index:
    email_id = df.iloc[row]['email']
    hash_object = hashlib.md5(email_id.encode())
    # Hexadecimal version of the md5hash
    hash_dict_ids[df.iloc[row]['userid']] = str(hash_object.hexdigest())
  
  # Upload hash dictionary to the directory
  with open('hash_dict_ids.json', 'w', encoding = 'utf-8') as f:
    json.dump(hash_dict_ids, f, ensure_ascii = False, indent = 4)
    
    
words2categories = {}
prefixes2categories = {}

def read_liwc_dictionary(liwc_fn):
    with open(liwc_fn) as csvfile:
        csvreader = csv.reader(csvfile)
        header = next(csvreader)
        for row in csvreader:
            for cat, term in zip(header, row):
                term = term.lower().strip()
                if not term:
                    continue
                global prefixes2categories, word2categories
                if ".*" in term:
                    # This is a prefix
                    prefix = term.replace('.*', '')
                    prefix2 = stemmer.stem(prefix)
                    prefixes2categories.setdefault(prefix2, []).append(cat)
                else:
                    # Full word
                    words2categories.setdefault(term, []).append(cat)


def get_categories_from_word(w):

    cats = []
    if w in words2categories:
        cats += words2categories[w]
    # Check if stem is in prefixes
    pref = stemmer.stem(w)
    if pref in prefixes2categories:
        cats += prefixes2categories[pref]
    cats = list(set(cats))
    return cats



def word_to_liwc_cats(words):
    cats = [c for w in words for c in get_categories_from_word(w)]
    return cats



def liwc_cats_to_dict(cats):
    countdict = Counter(cats)
    return dict(sorted(countdict.items(), key=itemgetter(1), reverse=True))



def body_to_liwc(cleaned_toks):
  liwc_cat_counts = liwc_cats_to_dict(word_to_liwc_cats(cleaned_toks))
  return liwc_cat_counts
