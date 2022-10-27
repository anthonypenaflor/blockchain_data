# blockchain_data
Aggregating Web3 blockchain data and loading into a database for easy access

To run this program, have a node addressa and insert into:
- w3

And a database ready to insert into:
- db_name
- pw 
- Client(['your_cloud']...)

Once the credentials have been inputted, running the script will collect the first one hundred thousand blocks and transactions for Arbitrum, and then upload them to the database provided. This can also be changed by updating the 'first_100k' variable in 'main'.
