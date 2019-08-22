//The plot 
var t_Chart = echarts.init(document.getElementById('t_chart'));
var f_Chart = echarts.init(document.getElementById('f_chart'));
//max label range size
var maxsize_t = 240;
var maxsize_f = 600;

//chart config of queues' traffic 
traffic_option = {
    title: {
        text: 'Traffic of each queue'
    },
    tooltip: {
        trigger: 'axis'
    },
    xAxis: {
        type: 'value',
        splitLine: {
            show: false
        },
        min: 0,
        max: maxsize_t/2
    },
    yAxis: {
        type: 'value'
    },
    series: []
};

//chart config of each flow's traffic
flows_option = {
    title: {
        text: 'Traffic of each flow'
    },
    legend:{
        data:[]
    },
    tooltip: {
        trigger: 'axis'
    },
    xAxis: {
        type: 'value',
        splitLine: {
            show: false
        },
        min: 0,
        max: maxsize_t/2
    },
    yAxis: {
        type: 'value'
    },
    series: []
};

var index_list = []; //Save current stored queue index
var flow_list = []; //save current stored flow index

chartColors = {
    red: 'rgb(255, 99, 132)',
    blue: 'rgb(54, 162, 235)',
    green: 'rgb(75, 192, 192)',
	orange: 'rgb(255, 159, 64)',
	yellow: 'rgb(255, 205, 86)',
	purple: 'rgb(153, 102, 255)',
	grey: 'rgb(201, 203, 207)'
};

var colorNames = Object.keys(chartColors);   //for color changing

setInterval(function()
{
    $.get("http://localhost:17777",{},function(res){
        //extract information
        var timer = res['timer']/1000;
        timer = timer.toFixed(1);
        var flows = res['Active flows'];

        //update range of xAxis
        traffic_option.xAxis.max = parseInt(timer)+1;
        traffic_option.xAxis.min = parseInt(timer)>=maxsize_t/2?parseInt(timer)-maxsize_t/2:0;

        flows_option.xAxis.max = parseInt(timer)+1;
        flows_option.xAxis.min = parseInt(timer)>=maxsize_f/2?parseInt(timer)-maxsize_f/2:0;

        //update each queue
        for(var i = 0, len = flows.length; i < len; i++)
        {
            var bw = flows[i]['bw'] / 1000000;
            //for flows_option
            //update flows' stats
            var flow_5tuple =  flows[i]['src']+':'+flows[i]['sp']+'-'+flows[i]['dst']+':'+flows[i]['dp'];
            var f = flow_list.indexOf(flow_5tuple);
            if(f != -1)
            {
                flows_option.series[f].data.push([timer,bw]);
            }
            else
            {
                var colorName = colorNames[flow_list.length % colorNames.length];
                var newColor = chartColors[colorName];
                var new_data = {
                    name: flow_5tuple,
                    color: newColor,
                    type: 'line',
                    //stack:'flow',
                    //areaStyle: {},
                    data: [[timer,bw]]
                };                
                flows_option.series.push(new_data);
                flow_list.push(flow_5tuple);
                flows_option.legend.data.push(flow_5tuple);
            }

            //for traffic_option
            //the index of this flow in chart queue
            var j = index_list.indexOf(flows[i]['queue_index']);
            //if exist
            if(j != -1)
            {
                traffic_option.series[j].data.push([timer,bw]);
            }
            //not exist
            else
            {
                var colorName = colorNames[flows[i]['queue_index'] % colorNames.length];
                
                var newColor = chartColors[colorName];
                var new_data = {
                    name: 'Flow: ' + flows[i]['queue_index'],
                    color: newColor,
                    type: 'line',
                    data: [[timer,bw]]
                };
                traffic_option.series.push(new_data);
                //option.legend.data.push(new_data.name);
                index_list.push(flows[i]['queue_index']);
            }
        }

        //for flows_option
        for(var i = 0, len = flows_option.series.length; i < len; i++)
        {
            if(flows_option.series[i].data.length > maxsize_f*2)
            {
                flows_option.series[i].data.splice(0,maxsize_f);
                //flows_option.series[i].data.shift();
            }
            var l = flows_option.series[i].data.length; 
            if(flows_option.series[i].data[l-1][0] != timer)
            {
                flows_option.series[i].data.push([timer,0]);
            }
        }

        //for traffic_option
        //delete 
        for(var i = 0, len = traffic_option.series.length; i < len; i++)
        {
            if(traffic_option.series[i].data.length == 0)
            {
                traffic_option.series.splice(i,1);
                //option.legend.data.splice(i,1);
                index_list.splice(i,1);
                i -=1 ;
                len -=1 ;
            }
            else if(traffic_option.series[i].data[0][0] < parseInt(timer) - maxsize_t)
            {
                traffic_option.series[i].data.shift();
            }
        }
        console.log(flows_option.series);
        t_Chart.setOption(traffic_option);
        f_Chart.setOption(flows_option);
    });
}, 500);
