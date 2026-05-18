from google.cloud import storage

client = storage.Client()

print("\nAvailable Buckets:\n")

for bucket in client.list_buckets():
    print(bucket.name)