const mongoose = require('mongoose');

const { Schema } = mongoose;
const tweetSchema = new Schema({
  text: String,
  createdAt: String,
  country: String,
  location: String,
  placeType: String,
  emojis: [String],
  hashtags: [String],
  hour: String,
  minute: String,
  hourAndMinute: String,
  day: String,
  retweetCount: Number,
  replyCount: Number,
  quoteCount: Number,
  favoriteCount: Number,
  user: Object
});

const Tweet = mongoose.model('tweets-2', tweetSchema);

module.exports = Tweet;
