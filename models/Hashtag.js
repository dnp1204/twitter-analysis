const mongoose = require('mongoose');

const { Schema } = mongoose;

const hashtagSchema = new Schema({
  hashtag: String,
  count: Number
});

const Hashtags = mongoose.model('hashtags', hashtagSchema);

module.exports = Hashtags;
