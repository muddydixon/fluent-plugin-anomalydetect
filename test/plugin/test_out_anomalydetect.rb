require 'helper'

class AnomalyDetectOutputTest < Test::Unit::TestCase
  def setup
    Fluent::Test.setup
  end

  CONFIG = %[
    tag test.anomaly
    outlier_term 28
    outlier_discount 0.05
    score_term 28
    score_discount 0.05
    tick 10
    smooth_term 3
    target y
  ]
  
  def create_driver (conf=CONFIG, tag="debug.anomaly")
    Fluent::Test::OutputTestDriver.new(Fluent::AnomalyDetectOutput, tag).configure(conf)
  end

  def test_configure
    d = create_driver('')
    assert_equal 28, d.instance.outlier_term
    assert_equal 0.05, d.instance.outlier_discount
    assert_equal 7, d.instance.smooth_term
    assert_equal 14, d.instance.score_term
    assert_equal 0.1, d.instance.score_discount
    assert_equal 300, d.instance.tick
    assert_nil d.instance.target
    assert_equal 'anomaly', d.instance.tag

    d = create_driver
    assert_equal 28, d.instance.outlier_term
    assert_equal 0.05, d.instance.outlier_discount
    assert_equal 3, d.instance.smooth_term
    assert_equal 28, d.instance.score_term
    assert_equal 0.05, d.instance.score_discount
    assert_equal 10, d.instance.tick
    assert_equal "y", d.instance.target
    assert_equal 'test.anomaly', d.instance.tag

    assert_raise(Fluent::ConfigError) {
      d = create_driver %[
        outlier_discount 1.3
      ]
    }
    assert_raise(Fluent::ConfigError) {
      d = create_driver %[
        score_discount 1.3
      ]
    }
    assert_raise(Fluent::ConfigError) {
      d = create_driver %[
        outlier_discount -0.3
      ]
    }
    assert_raise(Fluent::ConfigError) {
      d = create_driver %[
        score_discount -0.3
      ]
    }
    assert_raise(Fluent::ConfigError) {
      d = create_driver %[
        outlier_term 0
      ]
    }
    assert_raise(Fluent::ConfigError) {
      d = create_driver %[
        score_term 0
      ]
    }
    assert_raise(Fluent::ConfigError) {
      d = create_driver %[
        smooth_term 0
      ]
    }
    assert_raise(Fluent::ConfigError) {
      d = create_driver %[
        tick 0
      ]
    }
    assert_raise(Fluent::ConfigError) {
      d = create_driver %[
        target y
        targets x,y,z
      ]
    }
    assert_raise(Fluent::ConfigError) {
      d = create_driver %[
        threshold 1.0
        thresholds 1.0,2.0
      ]
    }
    assert_raise(Fluent::ConfigError) {
      d = create_driver %[
        thresholds 1,2
      ]
    }
    assert_raise(Fluent::ConfigError) {
      d = create_driver %[
        targets x,y,z
        thresholds 1
      ]
    }
  end

  def test_emit_record_count
    d = create_driver %[
      tag test.anomaly
      outlier_term 28
      outlier_discount 0.05
      score_term 28
      score_discount 0.05
      tick 10
      smooth_term 3
    ]

    data = 10.times.map { (rand * 100).to_i } + [0]
    d.run do 
      data.each do |val|
        (0..val - 1).each do ||
          d.emit({'y' => 1})
        end
        r = d.instance.flush[:all]
        assert_equal val, r['target']
      end
    end
  end

  def test_emit_target
    d = create_driver %[
      tag test.anomaly
      outlier_term 28
      outlier_discount 0.05
      score_term 28
      score_discount 0.05
      tick 10
      smooth_term 3
      target y
    ]

    data = 10.times.map { (rand * 100).to_i } + [0]
    d.run do
      data.each do |val|
        d.emit({'y' => val})
        r = d.instance.flush[:all]
        assert_equal val, r['target']
      end
    end
  end

  def test_emit_when_target_does_not_exist
    d = create_driver %[
      tag test.anomaly
      outlier_term 28
      outlier_discount 0.05
      score_term 28
      score_discount 0.05
      tick 10
      smooth_term 3
      target y
    ]

    d.run do
      10.times do
        d.emit({'foobar' => 999.99})
        r = d.instance.flush[:all]
        assert_equal nil, r
      end
    end
  end

  def test_emit_stock_data
    require 'csv'
    reader = CSV.open("test/stock.2432.csv", "r")
    header = reader.take(1)[0]
    d = create_driver
    d.run do 
      reader.each_with_index do |row, idx|
        break if idx > 5
        d.emit({'y' => row[4].to_i})
        r = d.instance.flush[:all]
        assert r['target']
        assert r['outlier']
        assert r['score']
      end
    end
  end

  def test_store_file
    dir = "test/tmp"
    Dir.mkdir dir unless Dir.exist? dir
    file = "#{dir}/test.dat"
    File.unlink file if File.exist? file

    d = create_driver %[
      store_file #{file}
    ]

    d.run do
      assert_equal([], d.instance.outlier_bufs(:all))
      d.emit({'x' => 1})
      d.emit({'x' => 1})
      d.emit({'x' => 1})
      d.instance.flush[:all]
      d.emit({'x' => 1})
      d.emit({'x' => 1})
      d.emit({'x' => 1})
      d.instance.flush[:all]
    end
    assert File.exist? file

    d2 = create_driver %[
      store_file #{file}
    ]
    d2.run do
      assert_equal 2, d2.instance.outlier_bufs(:all).size
    end

    File.unlink file
  end

  def test_set_large_threshold
    require 'csv'
    reader = CSV.open("test/stock.2432.csv", "r")
    header = reader.take(1)[0]
    d = create_driver %[
      threshold 1000
    ]
    d.run do 
      reader.each_with_index do |row, idx|
        break if idx > 5
        d.emit({'y' => row[4].to_i})
        r = d.instance.flush[:all]
        assert_equal nil, r
      end
    end
  end

  def test_set_small_threshold
    require 'csv'
    reader = CSV.open("test/stock.2432.csv", "r")
    header = reader.take(1)[0]
    d = create_driver %[
      threshold 1
    ]
    d.run do 
      reader.each_with_index do |row, idx|
        break if idx > 5
        d.emit({'y' => row[4].to_i})
        r = d.instance.flush[:all]
        assert_not_equal nil, r
      end
    end
  end

  def test_up_trend
    d = create_driver %[
      target y
      trend up
    ]

    # should not output in down trend
    d.run do
      d.emit({'y' => 0.0}); d.instance.flush
      d.emit({'y' => 0.0}); d.instance.flush
      d.emit({'y' => 0.0}); d.instance.flush
      d.emit({'y' => -1.0}); r = d.instance.flush[:all]
      assert_equal nil, r
    end

    # should output in up trend
    d.run do
      d.emit({'y' => -1.0}); d.instance.flush
      d.emit({'y' => -1.0}); d.instance.flush
      d.emit({'y' => -1.0}); d.instance.flush
      d.emit({'y' => 0.0}); r = d.instance.flush[:all]
      assert_not_equal nil, r
    end
  end

  def test_down_trend
    d = create_driver %[
      target y
      trend down
    ]
    # should output in down trend
    d.run do
      d.emit({'y' => 0.0}); d.instance.flush
      d.emit({'y' => 0.0}); d.instance.flush
      d.emit({'y' => 0.0}); d.instance.flush
      d.emit({'y' => -1.0}); r = d.instance.flush[:all]
      assert_not_equal nil, r
    end

    # should not output in up tread
    d.run do
      d.emit({'y' => -1.0}); d.instance.flush
      d.emit({'y' => -1.0}); d.instance.flush
      d.emit({'y' => -1.0}); d.instance.flush
      d.emit({'y' => 0.0})
      r = d.instance.flush[:all]
      assert_equal nil, r
    end
  end

  def test_aggregate_tag
    d = create_driver %[
      outlier_term 28
      outlier_discount 0.05
      score_term 28
      score_discount 0.05
      tick 10
      smooth_term 3
      aggregate tag
      add_tag_prefix test
    ]

    data = 10.times.map { (rand * 100).to_i } + [0]
    d.run do 
      data.each do |val|
        (0..val - 1).each do ||
          d.emit({'y' => 1})
        end
        r = d.instance.flush['debug.anomaly']
        assert_equal val, r['target']
      end
    end
  end

  def test_targets
    d = create_driver %[
      targets x,y
    ]
    data = 10.times.map { (rand * 100).to_i } + [0]
    d.run do
      data.each do |val|
        d.emit({'x' => val, 'y' => val})
        r = d.instance.flush[:all]
        assert_equal val, r['x']
        assert_equal val, r['y']
      end
    end
  end

  def test_targets_default_suffix
    d = create_driver %[
      targets x,y
    ]
    data = 1.times.map { (rand * 100).to_i } + [0]
    d.run do
      data.each do |val|
        d.emit({'x' => val, 'y' => val})
        r = d.instance.flush[:all]
        assert r.has_key?('x')
        assert r.has_key?('y')
        assert r.has_key?('x_outlier')
        assert r.has_key?('x_score')
        assert r.has_key?('y_outlier')
        assert r.has_key?('y_score')
      end
    end
  end

  def test_targets_suffix
    d = create_driver %[
      targets x,y
      outlier_suffix
      score_suffix _anomaly
      target_suffix _target
    ]
    data = 1.times.map { (rand * 100).to_i } + [0]
    d.run do
      data.each do |val|
        d.emit({'x' => val, 'y' => val})
        r = d.instance.flush[:all]
        assert r.has_key?('x_target')
        assert r.has_key?('y_target')
        assert r.has_key?('x')
        assert r.has_key?('x_anomaly')
        assert r.has_key?('y')
        assert r.has_key?('y_anomaly')
      end
    end
  end

  def test_targets_thresholds
    d = create_driver %[
      targets x,y
      thresholds 1,2
    ]
    d.run do
      thresholds = d.instance.thresholds
      assert_equal 1, thresholds['x']
      assert_equal 2, thresholds['y']

      threshold_proc = d.instance.threshold_proc
      assert_equal 1, threshold_proc.call('x')
      assert_equal 2, threshold_proc.call('y')
    end
  end
end
