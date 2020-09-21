# Sendit

A command line utility to send email batches with sendgrid - create email batches and send them with confidence!

## Usage

```
python sendit.py [verb] [arguments]
```

Verbs:

* `list [all|batchid]`: list active batches. If `all`, also lists inactive batches. If `batchid`, give information about this batch.
* `create batchid template_key`: create a batch with a given id
* `add batchid filename.csv`: add to a batch the emails the info in filename.csv. The info in filename.csv may include:
     * to_name
     * to_email
     * cc_name
     * cc_email
     * any variables that need to be replaced.
* `test batchid my@email.com`: sends one test email to my@email.com with the information available from the first line in the csv. 
* `send batchid [(0-9)+%?|all]`: send a certain number or percentage of emails from the batch. 



