const mongoose = require('mongoose');

const { Schema } = mongoose;

const hourHashtagSchema = new Schema({
  hashtag: String,
  hour: String,
  count: Number
});

const HourHashtag = mongoose.model('hour_hashtags', hourHashtagSchema);

module.exports = HourHashtag;
