const { createLogger, format, transports } = require('winston');
require('winston-daily-rotate-file');
const { combine, timestamp, simple, splat, json, colorize, printf, prettyPrint } = format;


const myFormat = printf(info => {
  return `${info.timestamp} ${info.level}: ${info.message} ${info.meta || ''}`;
});

const logger = createLogger({
  level: process.env.LOG_LEVEL || process.env.NODE_ENV == 'production' ? 'info': 'debug',
  format: combine(
    json(),
    splat(),
    timestamp(),
    // prettyPrint(),
    myFormat
  ),
  transports: [
    //
    // - Write to all logs with level `info` and below to `combined.log`
    // - Write all logs error (and below) to `error.log`.
    //
    // new transports.File({ filename: 'amqp.error.log', level: 'error' }),
    // new transports.File({ filename: 'amqp.combined.log' })
    new transports.DailyRotateFile({
      filename: 'logs/%DATE%.combined.log',
      datePattern: 'YYYY-MM-DD-HH',
      maxSize: '20m',
      maxFiles: '14d',

    }),
    new transports.DailyRotateFile({
      filename: 'logs/%DATE%.error.log',
      datePattern: 'YYYY-MM-DD-HH',
      maxSize: '20m',
      maxFiles: '14d',
      level: 'error'
    })
  ]
});

//
// If we're not in production then log to the `console` with the format:
// `${info.level}: ${info.message} JSON.stringify({ ...rest }) `
//
if (process.env.NODE_ENV !== 'production') {
  logger.add(new transports.Console({
    'colorize':true,
    format: combine(
      colorize({all:true}),
      json(),
      prettyPrint(),
      myFormat,
    ),
  }));
}

module.exports = logger;