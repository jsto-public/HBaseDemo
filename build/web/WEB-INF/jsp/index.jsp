<!DOCTYPE HTML>
<html>
    <head>
        <title>HBaseDemo</title>
        <meta charset="utf-8" />
        <meta name="viewport" content="width=device-width, initial-scale=1" />
        <!--[if lte IE 8]><script src="assets/js/ie/html5shiv.js"></script><![endif]-->
        <link rel="stylesheet" href="assets/css/main.css" />
        <!--[if lte IE 9]><link rel="stylesheet" href="assets/css/ie9.css" /><![endif]-->
        <script type="text/javascript" src="http://ajax.googleapis.com/ajax/libs/jquery/1.8.2/jquery.min.js"></script>
    </head>
    <body>
        <!-- Banner -->
        <section id="banner">
            <h2>HBase<strong>Demo</strong>
        </section>
        <section id="one" class="wrapper special">
            <div class="inner">
                <p>This test project has simulated data for 160,000 meters each storing a year's worth of 8760 hourly interval data for a total of about 1.5 billion pieces of interval data. It is running on a Hadoop+HBase cluster in AWS.</p>
                <p>To retrieve data for a random meter, press the button below.</p>
                <form class="grid-form" method="post" action="#">
                    <div class="form-control narrow">
                        <div id="getDataButton" class="button special">Get Data</div>
                    </div>
                </form>
                <div id="timeResult"></div>
        </section>
        <div style="clear:both"></div>
        <script src="assets/js/highstock.js"></script>
        <script src="assets/js/modules/exporting.js"></script>
        <div id="container" style="height: 400px; min-width: 310px; padding-left: 150px; padding-right: 150px"></div>
        <!-- Footer -->
        <footer id="footer">
        </footer>   
        <script>
            var meterId;
            var startTime;
            var endTime;
            function getRandomInt(min, max) {
               return Math.floor(Math.random() * (max - min + 1)) + min;
            }
            $(document).ready(function () {
                $("#getDataButton").click(function () {
                   // $.getJSON('http://www.highcharts.com/samples/data/jsonp.php?filename=aapl-c.json&callback=?', function (data) {
                   var power = getRandomInt(0,5);
                   meterId = Math.pow(10,power);
                   startTime = new Date().getTime();
                   $.ajax({url: "GetData.htm?meterId=" + meterId}).done( function (datastr) {
                       endTime = new Date().getTime();
                       $( "#timeResult" ).text('REST API returned interval data in ' + (endTime - startTime) + ' ms');
                        dataobj = $.parseJSON(datastr);
                        data = dataobj.data;
                        
                        // Create the chart
                        $('#container').highcharts('StockChart', {
                            rangeSelector: {
                                selected: 1
                            },
                            title: {
                                text: 'Interval data for MeterId ' + meterId
                            },
                            series: [{
                                    name: 'kWh',
                                    data: data,
                                    tooltip: {
                                        valueDecimals: 2
                                    }
                                }]
                        });
                    });

                });

            });
        </script>
    </body>
</html>