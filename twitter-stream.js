const _ = require('lodash');
const { onlyEmoji } = require('emoji-aware');
const mongoose = require('mongoose');
const Twitter = require('twitter');

const keys = require('./keys');

const Tweet = require('./models/Tweet');

mongoose.connect(
  keys.mongoURI,
  { useNewUrlParser: true },
  err => {
    if (err) {
      console.log(err);
    } else {
      const client = new Twitter({
        consumer_key: keys.apiKey,
        consumer_secret: keys.apiSecret,
        access_token_key: keys.accessToken,
        access_token_secret: keys.accessTokenSecret
      });

      const stream = client.stream('statuses/filter', {
        locations: '-124.848974,24.396308,-66.885444,49.384358'
      });

      stream.on('data', event => {
        const isTweet = _.conforms({
          user: _.isObject,
          id_str: _.isString,
          text: _.isString
        });

        if (isTweet(event)) {
          const {
            text,
            created_at,
            retweet_count,
            reply_count,
            quote_count,
            favorite_count,
            user,
            place: { country, full_name, place_type }
          } = event;

          const time = created_at.split(' ');
          const day = time[0];
          const [hour, minute] = time[3].split(':');

          const hashtags = text
            .split(' ')
            .filter(word => {
              if (word.startsWith('#') && word.length > 1) {
                return word;
              }
            })
            .map(word => {
              return word.toLowerCase().replace(/[^a-zA-Z0-9# ]/g, '');
            });
          const emojis = onlyEmoji(text);
          const data = {
            text,
            createdAt: created_at,
            country,
            location: full_name,
            placeType: place_type,
            hour,
            minute,
            day,
            retweetCount: parseInt(retweet_count, 10),
            replyCount: parseInt(reply_count, 10),
            quoteCount: parseInt(quote_count, 10),
            favoriteCount: parseInt(favorite_count, 10),
            user,
            hourAndMinute: `${hour}:${minute}`
          };

          if (hashtags.length > 0) {
            data.hashtags = hashtags;
          }

          if (emojis.length > 0) {
            data.emojis = emojis;
          }

          Tweet.create(data)
            .then(() => {
              console.log(`Save tweet ${event.text} to database`);
            })
            .catch(error => {
              console.log(error);
            });
        }
      });

      stream.on('error', error => {
        console.log(error);
      });
    }
  }
);
