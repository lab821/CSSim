//max label range size
var maxsize = 60;

//chart config
var config = {
    type: 'line',
    data: {
        labels: new Array(maxsize),
        datasets: []
    },
    options: {
        responsive: true,
        title: {
            display: true,
            text: 'Active flows forwarding rate'
        },
        tooltips: {
            mode: 'index',
            intersect: false,
        },
        hover: {
            mode: 'nearest',
            intersect: true
        },
        scales: {
            xAxes: [{
                display: true,
                scaleLabel: {
                    display: true,
                    labelString: 'Timer'
                }
            }],
            yAxes: [{
                display: true,
                scaleLabel: {
                    display: true,
                    labelString: 'Value'
                }
            }]
        }
    }
};

//The plot 
window.onload = function() {
    var ctx = document.getElementById('canvas').getContext('2d');
    window.myLine = new Chart(ctx, config);
};

var index_list = []; //Save current stored flow index

window.chartColors = {
	red: 'rgb(255, 99, 132)',
	orange: 'rgb(255, 159, 64)',
	yellow: 'rgb(255, 205, 86)',
	green: 'rgb(75, 192, 192)',
	blue: 'rgb(54, 162, 235)',
	purple: 'rgb(153, 102, 255)',
	grey: 'rgb(201, 203, 207)'
};

var colorNames = Object.keys(window.chartColors);   //for color changing

function requestinfo()
{
    $.get("http://localhost:17777",{},function(res){
        var timer = res['timer']/1000;
        timer = timer.toFixed(1);
        var flows = res['Active flows'];

        config.data.labels.push(timer);
        if(config.data.labels.length > maxsize){
            config.data.labels.splice(0,1);
        }

        for(var i = 0, len = flows.length; i < len; i++)
        {
            var j = index_list.indexOf(flows[i]['queue_index'])
            var bw = flows[i]['bw'] / 1000000
            if(j != -1)
            {
                config.data.datasets[j].data.push(bw);
            }
            else
            {
                var colorName = colorNames[config.data.datasets.length % colorNames.length];
                
                var newColor = window.chartColors[colorName];
                var data = {
                    label: 'Flow: ' + flows[i]['queue_index'],
                    backgroundColor: newColor,
                    borderColor: newColor,
                    data: new Array(maxsize),
                    fill: false,
                };
                data.data.push(bw);
                config.data.datasets.push(data);
                index_list.push(flows[i]['queue_index'])
            }
        }

        for(var i = 0, len = config.data.datasets.length; i < len; i++)
        {
            if(config.data.datasets[i].data.length == 0)
            {
                config.data.datasets.splice(i,1);
                index_list.splice(i,1);
                i -=1 ;
                len -=1 ;
                window.myLine.update();

            }
            else
            {
                config.data.datasets[i].data.splice(0,1);
            }
            //console.log(config.data.datasets[i])

        }

        window.myLine.update();
        setTimeout(requestinfo,1000);
    });
};

requestinfo();
