const mongoose = require('mongoose');

const { Schema } = mongoose;
const wordSchema = new Schema({
  word: String,
  count: Number
});

const Words = mongoose.model('important-words', wordSchema);

module.exports = Words;
