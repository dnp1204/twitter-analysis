const mongoose = require('mongoose');

const { Schema } = mongoose;

const averageSchema = new Schema({
  word_avg: Number,
  emoji_avg: Number,
  location_avg: Number,
  hashtags_avg: Number,
  important_words_avg: Number
});

const Average = mongoose.model('average_counts', averageSchema);

module.exports = Average;
