const mongoose = require('mongoose');

const { Schema } = mongoose;

const locationSchema = new Schema({
  location: String,
  count: Number
});

const Locations = mongoose.model('locations', locationSchema);

module.exports = Locations;
