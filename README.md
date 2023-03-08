Here is a description of the algorithm I used to generate it.
 
Take all of the heartbeat data from this report: https://cloudsi.sugarondemand.com/#Reports/5dbd1c4a-9adb-11ed-9bbe-029395602a62
Export it to a csv file, with data from today.
 
For each account, select the single best heartbeat for the account using this algorithm:
Is there an instance in SugarCloud using this license key, if so, use it
If not, find the installation with the highest number of users, if there’s still a tie, select the installation that is most recent
 
I then took each of those “best” Sugar Installations and looked up the ISP.
 
If they were SugarCloud, I marked hosting as such
If not, and they were in one of the bigger clouds, Amazon, MSFT, Google, IBM or Digital Ocean, I marked hosting as such
For all instances that didn’t fall into the above buckets, I sorted them based on the partner of record
If that partner had more than 3 customers hosted in that same ISP, I marked those customers as Partner Hosted.
 
Column C contains account ID for you to join against
Column N contains the ISP name I looked up using the whois database
Column O displays the likely hosting based on the algorithm above
Column P is a simple x if cloud or blank if on-premise
