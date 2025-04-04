from kafka import KafkaAdminClient
import os, csv


# Export and use proper bootstrap_servers and topic name
def bt_server_status_topic(btstrap_server, topic_name):
    bt_server_txt = os.environ.get(f'{btstrap_server}')
    topic_name = os.environ.get(f'{topic_name}')

    # Split bootstrap server to a list
    bt_server_lst = bt_server_txt.split(',')

    
    return  bt_server_lst, topic_name

bootstrap_server = "STAGE_AP_BOOTSTRAP_SERVER"
status_topic_env = "STAGE_AP_ASSOC_STATUS_TOPIC"

result = bt_server_status_topic(bootstrap_server, status_topic_env) 
bt_server = result[0]
status_topic = result[1]



admin_client = KafkaAdminClient(bootstrap_servers=bt_server)

consumer_groups = admin_client.list_consumer_groups()

group_ids = [group[0] for group in consumer_groups]  # Extract group IDs
group_details = admin_client.describe_consumer_groups(group_ids[5])

print(group_details)

# Inspect the details
# for group in group_details:
#     print(f"Group: {group.group_id}")
#     print(f"State: {group.state}")
#     print(f"Members: {len(group.members)}")


# try:
#     # consumer_group_lst = [group for group, _ in consumer_groups if ('ffer' or 'associ') in group]
#     # print(consumer_group_lst)
#     with open('offers-kafka-consumers.csv', 'w') as f:
#         headers = ['Consumer Names']
#         writer = csv.DictWriter(f, fieldnames=headers)
#         writer.writeheader()
        
#         for group, _ in consumer_groups:
#             if ('ffer' or 'associ') in group:
#                 new_record = {
#                     "Consumer Names": group
#                 }
#                 writer.writerow(new_record)
        
# finally:    
#     admin_client.close()
