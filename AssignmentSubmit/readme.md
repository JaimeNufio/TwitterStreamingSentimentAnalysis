GetTweets.py -
Uses twitter API to fetch tweets, clean them, and send them to a socket

ProcessTweets.py -
Trains Model based on training data, once done, reads tweets from socket, and passes them through the pipline. Then sends the resulting prediction and text to firebase for storage, fulfilling the Database requirement.

Model Design V2.ipynb - 
The "sandbox" I designed the Model in, or at least the 2nd one where I tried to clean it up in.

Screenshots -
A folder with 3 screenshots:
- Terminal where tweets are read and cleaned prior to beind sent to the next phase
- Terminal where tweets are read and predictions are made. 
- Image of firebase where tweets and their predicted sentiment are stored.