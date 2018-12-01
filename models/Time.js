const mongoose = require('mongoose');

const { Schema } = mongoose;

const timeSchema = new Schema({
  hour: String,
  minute: String,
  count: Number
});

const Times = mongoose.model('times', timeSchema);

module.exports = Times;
