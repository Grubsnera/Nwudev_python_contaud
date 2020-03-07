from twilio.rest import Client


# Your Account Sid and Auth Token from twilio.com/console
# DANGER! This is insecure. See http://twil.io/secure
account_sid = 'AC5dd70f4554e5b8ab34798bfcc91bd5b3'
auth_token = 'f9fde5a696576045985bd6287ebb6ef2'
client = Client(account_sid, auth_token)

message = client.messages.create(
                              body='Hello there!',
                              from_='whatsapp:+14155238886',
                              to='whatsapp:+27828812310'
                          )

print(message.sid)
