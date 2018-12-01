const Tweet = require('./models/Tweet');
const mongoose = require('mongoose');
const keys = require('./keys');
const { Promise } = require('bluebird');

mongoose.Promise = Promise;

mongoose.connect(
  keys.mongoURI,
  { useNewUrlParser: true },
  err => {
    if (err) {
      console.log(err);
    } else {
      (async function() {
        const data = await Tweet.find({
          hashtags: { $exists: true },
          $where: 'this.hashtags.length>0'
        });
        updateHashtags(data);
      })();
    }
  }
);

const updateHashtags = tweets => {
  return Promise.map(
    tweets,
    async tweet => {
      const newHashtags = tweet.hashtags.map(word => {
        return word.toLowerCase().replace(/[^a-zA-Z0-9# ]/g, '');
      });
      const newTweet = await Tweet.findByIdAndUpdate(tweet._id, {
        hashtags: newHashtags
      });
      console.log(newTweet.hashtags);
    },
    { concurrency: 100 }
  );
};
