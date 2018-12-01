const express = require('express');
const mongoose = require('mongoose');
const bodyParser = require('body-parser');

const keys = require('./keys');

const Word = require('./models/Word');
const Hashtag = require('./models/Hashtag');
const Location = require('./models/Location');
const Emoji = require('./models/Emoji');
const Time = require('./models/Time');

const app = express();

mongoose.connect(
  keys.mongoURI,
  { useNewUrlParser: true },
  err => {
    if (err) {
      console.log(err);
    }
  }
);

app.use(bodyParser.urlencoded({ extended: false }));
app.use(bodyParser.json());

app.get('/api/words/count', async (req, res) => {
  const { limit } = req.params;
  const data = await Word.find({})
    .sort({ count: -1 })
    .limit(limit || 10);

  res.send({ words: data });
});

app.get('/api/hashtags/count', async (req, res) => {
  const { limit } = req.params;
  const data = await Hashtag.find({})
    .sort({ count: -1 })
    .limit(limit || 10);

  res.send({ hashtags: data });
});

app.get('/api/locations/count', async (req, res) => {
  const { limit } = req.params;
  const data = await Location.find({})
    .sort({ count: -1 })
    .limit(limit || 10);

  res.send({ locations: data });
});

app.get('/api/emojis/count', async (req, res) => {
  const { limit } = req.params;
  const data = await Emoji.find({})
    .sort({ count: -1 })
    .limit(limit || 10);

  res.send({ emojis: data });
});

app.get('/api/times/count', async (req, res) => {
  const data = await Time.find({}).sort({ hour: 1, minute: 1 });

  res.send({ times: data });
});

app.listen(process.env.PORT || 5000, () => {
  console.log(`Server is on ${process.env.PORT || 5000}`);
});
