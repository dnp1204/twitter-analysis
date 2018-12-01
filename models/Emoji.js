const mongoose = require('mongoose');

const { Schema } = mongoose;

const emojiSchema = new Schema({
  emoji: String,
  count: Number
});

const Emojis = mongoose.model('emojis', emojiSchema);

module.exports = Emojis;
