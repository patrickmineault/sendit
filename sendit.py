import argparse
import chevron
import collections
import datetime
import dotenv
import hashlib
import json
import os
import pandas as pd
import python_http_client
from sendgrid import SendGridAPIClient
import base64
import tabulate
import tinydb
import itertools


def get_sg():
    sg = SendGridAPIClient(os.environ.get('SENDGRID_API_KEY'))

    # Send a no-op test request to make sure that keys are valid.
    try:
        response = sg.client.categories.get()
    except python_http_client.exceptions.ForbiddenError:
        reason = "You do not have sufficient permissions to access the Sengrid API. Put your API key `SENDGRID_API_KEY` in the .env file."
        raise Exception(reason)

    return sg


def get_db():
    return tinydb.TinyDB('.emaildb')


def get_template_tokens(template_key):
    """Get the replaceable tokens inside of a template.

    Args:
        template_key: the key of the template to examine

    Returns:
        A list of tokens inside the template.
    """
    try:
        response = get_sg().client.templates._(template_key).get()
    except python_http_client.exceptions.NotFoundError:
        raise Exception(f"The template `{template_key}`` does not exist")

    template = json.loads(response.body)
    html_content = template['versions'][-1]['html_content']
    subject = template['versions'][-1]['subject']

    tokens = chevron.tokenizer.tokenize(html_content)
    subject_tokens = chevron.tokenizer.tokenize(subject)

    variables = []
    for token_type, token in itertools.chain(tokens, subject_tokens):
        if token_type == 'variable':
            # This is replaceable token.
            sub_tokens = token.split()
            if sub_tokens[0] == 'insert':
                variables.append(sub_tokens[1])
            else:
                variables.append(sub_tokens[0])
    return variables


def create_batch(batch_id, template_key):
    # Create a batch with the right name.
    # Make sure to validate batch_id and template_key.
    db = get_db()
    q = tinydb.Query()
    if db.table('batches').search(q.batch_id == batch_id):
        raise Exception(f"Batch id {batch_id} already exists")

    tokens = get_template_tokens(template_key)

    # Nothing bad happened, proceed.
    db.table('batches').insert(
        {'batch_id': batch_id,
         'template_key': template_key,
         'tokens': tokens}
    )


def list_batches(which):
    """
    List batches on the command line

    Args:
        which: which batch to include in the list. Can be empty, all, or batch_id.

    """
    q = tinydb.Query()
    db = get_db()
    batches = db.table('batches')
    if which not in ('active', 'all'):
        batches = batches.search(q.batch_id == which)

    data = []
    for batch in batches:
        added = db.table('requests').count(
            q.batch_id == batch['batch_id']
        )
        
        sent = db.table('requests').count(
            (q.batch_id == batch['batch_id']) &
            (q.sent == True)
        )

        data.append({'batch_id': batch['batch_id'],
                     'added': added,
                     'sent': sent,
                     'tokens': batch['tokens']})
    
    print(
        tabulate.tabulate(
            list([[x['batch_id'], x['added'], x['sent'], x['tokens']] for x in data]),
            headers=['batch_id', 'added', 'sent', 'tokens'],
        )
    )

def get_digest(item):
    """
    Generate a stable digest for a dict.

    Args:
        item: a dict

    Returns:
        a hash for the item.
    """
    the_str = json.dumps(item, sort_keys=True)
    return hashlib.sha256(the_str.encode('utf-8')).hexdigest()


def add_to_batch(batch_id, items):
    """
    Add items to a batch with a given batch id

    Args:
        batch_id: the batch id
        items: a list of dicts
    """
    db = get_db()
    q = tinydb.Query()
    batch = db.table('batches').search(q.batch_id == batch_id)
    if not batch:
        raise Exception(f"Batch id {batch_id} does not exist!")
    batch = batch[0]

    bad_tokens = collections.defaultdict(lambda: 0)

    for item in items:
        for key in batch['tokens'] + ['categories']:
            if key not in item:
                bad_tokens[key] += 1

    # Check for bad tokens
    warning = ""
    for k, v in bad_tokens.items():
        if v > 0:
            warning += f"{k}: {v} missing values\n"
    
    if warning != '':
        print("Some tokens are missing from the values to be added.")
        print(warning)
        to_proceed = input('Proceed ([n]/y) ?')
        if to_proceed.lower() != 'y':
            raise Exception("User aborted import")


    for item in items:
        for key in batch['tokens']:
            if key not in item:
                bad_tokens[key] += 1

        item_ = item.copy()
        item_['batch_id'] = batch_id
        digest = get_digest(item_)
        if db.table('requests').contains(q.digest == digest):
            raise Exception("Cannot add duplicate row")

        if 'from_email' not in item_:
            raise Exception("Must use a from_email")
        if 'from_name' not in item_:
            raise Exception("Must use a from_name")
        if 'to_email' not in item_:
            raise Exception("Must use a to_email")
        
        item_['added'] = str(datetime.datetime.now())
        item_['sent'] = False
        item_['digest'] = digest

        db.table('requests').insert(item_)

def add_csv(batch_id, csv):
    df = pd.read_csv(csv)
    items = df.to_dict(orient='records')
    add_to_batch(batch_id, items)


def send_email(item, template_id):
    """
    Send one email item.

    Args:
        item: a dict containing the information for a given email.
        template_id: the template id to use.
    """
    if 'to_name' in item:
        to = {'email': item['to_email'], 'name': item['to_name']}
    else:
        to = {'email': item['to_email']}

    data = {
        'personalizations': [
            {
                "to": [to],
                'dynamic_template_data': item,
            },
        ],
        'template_id': template_id,
        'from': {
            'email': item['from_email'],
            'name': item['from_name']
        },
    }


    if 'attachment' in item:
        with open(item['attachment'],'rb') as f:
            attach_data = f.read()
            f.close()
        encoded_file = base64.b64encode(attach_data).decode()
        
        data['attachments'] = [
            {
                'content':encoded_file,
                'filename':item['attachment']
            },
            ]

    data['categories'] = item['categories'].split(',')

    get_sg().client.mail.send.post(request_body=data)
    return True


def send_test(batch_id, email):
    db = get_db()
    q = tinydb.Query()
    batch = db.table('batches').search(q.batch_id == batch_id)
    if not batch:
        raise Exception("batch id does not exist!")
    batch = batch[0]

    first_email = db.table('requests').search(q.batch_id == batch_id)[0]
    if 'cc_email' in first_email:
        del first_email['cc_email']
    if 'cc_name' in first_email:
        del first_email['cc_name']
    if 'to_name' in first_email:
        del first_email['to_name']
    first_email['to_email'] = email
    send_email(first_email, batch['template_key'])




def remove_batch(batch_id):
    db = get_db()
    q = tinydb.Query()
    db.table('batches').remove(q.batch_id == batch_id)
    db.table('requests').remove(q.batch_id == batch_id)


def list_templates():
    response = get_sg().client.templates.get()
    print(response.status_code)
    print(response.headers)
    print(response.body)
    templates = json.loads(response.body)
    data = []
    for template in templates:
        data.append({'name': template['name'], 'id': template['id'], 'date': template['date']})

    print(
        tabulate.tabulate(
            list([[x['name'], x['id'], x['date']] for x in data]),
            headers=['name', 'id', 'date'],
        )
    )

def send_batch(batch_id, how_many):
    """
    Sends (part of ) a batch of emails

    Args:
        batch_id: the batch id
        how_many: what number of emails to send (can be an int, a percentage string, or the string `all`)

    Side effects: marks the email as sent.
    """
    db = get_db()
    q = tinydb.Query()

    batch = db.table('batches').get(q.batch_id == batch_id)
    emails = db.table('requests').search(q.batch_id == batch_id)

    # Parse how_many
    if how_many[-1] == '%':
        # Parse how many as a percentage
        percentage = float(how_many[:-1] / 100)
        total_num = int(percentage * len(emails))
    elif how_many == 'all':
        total_num = sum([1 for x in emails if not x['sent']])
    else:
        total_num = int(how_many)

    # And send away!
    sent = 0
    chars = "|\\-/"
    for email in emails:
        if email['sent']:
            continue

        print(f"\r{chars[sent % 4]} Sending email {sent + 1}/{total_num}", end="")

        send_email(email, batch['template_key'])
        db.table('requests').update(
            {'sent': True}, 
            q.digest == email['digest']
        )

        sent += 1

        if sent >= total_num:
            break
    
    print("")

def main():
    parser = argparse.ArgumentParser(description='Manage sendgrid email batches with confidence')
    subparser = parser.add_subparsers(dest='verb')

    list_parser = subparser.add_parser('list', help='List batches')
    list_parser.add_argument("which", nargs='?', default='active', help='Which batches to list (active, all, batch_id), default active')

    create_parser = subparser.add_parser('create', help='Create a new batch')
    create_parser.add_argument("batch_id", help='Batch id')
    create_parser.add_argument("template_key", help='Template key')

    add_parser = subparser.add_parser('add', help='Adds a set of information to a batch')
    add_parser.add_argument("batch_id", help='Batch id')
    add_parser.add_argument("csv", help='CSV file')

    template_parser = subparser.add_parser('templates', help='List templates')

    test_parser = subparser.add_parser('test', help='Sends a test email')
    test_parser.add_argument("batch_id", help='Batch id')
    test_parser.add_argument("to_email", help='Email')

    remove_parser = subparser.add_parser('remove', help='Deletes an email batch')
    remove_parser.add_argument("batch_id", help='Batch id')

    send_parser = subparser.add_parser('send', help='Sends an email batch')
    send_parser.add_argument("batch_id", help='Batch id')
    send_parser.add_argument("how_many", default='all', nargs='?', help='How many of the emails in the batch to send, all|[0-9]+%?')

    opts = parser.parse_args()

    if opts.verb == 'list':
        list_batches(opts.which)
    elif opts.verb == 'create':
        create_batch(opts.batch_id, opts.template_key)
    elif opts.verb == 'add':
        add_csv(opts.batch_id, opts.csv)
    elif opts.verb == 'remove':
        remove_batch(opts.batch_id)
    elif opts.verb == 'test':
        send_test(opts.batch_id, opts.to_email)
    elif opts.verb == 'send':
        send_batch(opts.batch_id, opts.how_many)
    elif opts.verb == 'templates':
        list_templates()
    else:
        parser.print_help()

if __name__ == '__main__':
    dotenv.load_dotenv()
    main()
