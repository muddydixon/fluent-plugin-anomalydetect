# Fluent::Plugin::Anomalydetect, a plugin for [Fluentd](http://fluentd.org) [![Build Status](https://travis-ci.org/muddydixon/fluent-plugin-anomalydetect.png?branch=master)](https://travis-ci.org/muddydixon/fluent-plugin-anomalydetect)

To detect anomaly for log stream, use this plugin.
Then you can find changes in logs casually.

## Installation

Add this line to your application's Gemfile:

    gem 'fluent-plugin-anomalydetect'

And then execute:

    $ bundle

Or install it yourself as:

    $ gem install fluent-plugin-anomalydetect

## Usage

    <source>
      type file
      ...
      tag access.log
    </source>

    <match access.**>
      type anomalydetect
      tag anomaly.access
      tick 86400
    </match>

    <match anomaly.access>
      type file
      ...
    </match>

Then the plugin output anomaly log counts in each day.

This plugin watches a value of input record number in the interval set with `tick`.

If you want to watch a value for a target field <fieldname> in data, write below:

    <match access.**>
      type anomalydetect
      tag anomaly.access
      tick 86400
      target fieldname
    </match>

## more configuration

    <match access.**>
      type anomalydetect
      tag anomaly.access
      tick 86400
      target fieldname
      outlier_term 7
      outlier_discount 0.5
      smooth_term 7
      score_term 28
      score_discount 0.01
    </match>

If you want to know detail of these parameters, see "Theory".

    <match access.**>
      type anomalydetect
      ...
      store_file /path/to/anomalydetect.dat
    </match>

If "store_file" option was specified, a historical stat will be stored to the file at shutdown, and it will be restored on started.


    <match access.**>
      type anomalydetect
      ...
      threshold 3
    </match>

If "threshold" option was specified, plugin only ouput when the anomalyscore is more than threshold.

    <match access.**>
      type anomalydetect
      ...
      trend up
    </match>

If "trend" option was specified, plugin only ouput when the input data tends to up (or down).

## Parameters

- outlier\_term

- outlier\_discount

- smooth\_term

- score\_term

- score\_discount

- tick

    The time interval to watch in seconds.

- tag

    The output tag name. Required for aggregate `all`. Default is `anomaly`.

- add_tag_prefix

    Add tag prefix for output message. Required for aggregate `tag`.

- remove_tag_prefix

    Remove tag prefix for output message.

- aggragate

    Process data for each `tag` or `all`. The default is `all`.

- target

    Watch a value of a target field in data. If not specified, the number of records is watched (default). The output would become like:

        {"outlier":1.783,"score":4.092,"target":10}

- threshold

    Emit message only if the score is greater than the threshold. Default is `-1.0`.

- trend

    Emit message only if the input data trend is `up` (or `down`). Default is nil.

- store\_file

    Store the learning results into a file, and reload it on restarting.

- targets

    Watch target fields in data. Specify by comma separated value like `x,y`. The output messsages would be like:

        {"x_outlier":1.783,"x_score":4.092,"x":10,"y_outlier":2.310,"y_score":3.982,"y":3}

- thresholds

    Threahold values for each target. Specify by comma separated value like `1.0,2.0`. Use with `targets` option.

- outlier\_suffix

    Change the suffix of emitted messages of `targets` option. Default is `_outlier`.

- score\_suffix

    Change the suffix of emitted messages of `targets` option. Default is `_score`.

- target\_suffix

    Change the suffix of emitted messages of `targets` option. Default is `` (empty).

- suppress\_tick

    Suppress to emit output messsages during specified seconds after starting up.


## Theory
"データマイニングによる異常検知" http://amzn.to/XHXNun

# ToDo

## FFT algorithms

# Copyright

* Copyright

  * Copyright (c) 2013- Muddy Dixon
  * Copyright (c) 2013- Naotoshi Seo

* License

  * Apache License, Version 2.0
