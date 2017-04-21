var dataset;

function getClusterDBs()
{
	$.getJSON("http://localhost:3000/dm/dbl", function (databaseList)
	{
		var dbdd = document.getElementById('databaseDD');
		var dbLen = databaseList.length;
		console.log("DB Len = " + dbLen);
		console.log(databaseList);
		$.each (databaseList, function (i, field)
		{
			if (i > 0)
				dbdd.options[i] = new Option(field.name, field.name);
		});
	});
}

function run_cluster_python()
{
	var selected_db = document.getElementById('databaseDD').value;
	var selected_tb = document.getElementById('tableDD').value;



}

function getClusterTables()
{
	var dbddVal = document.getElementById('databaseDD').value;
	$.ajax({
		url:'http://localhost:3000/dm/tbl',
		type:'post',
		data:{db: dbddVal}
	});
	$.getJSON("http://localhost:3000/dm/tbl", function (tableList)
	{
		var tbdd = document.getElementById('tableDD');
		var tbLen = tableList.length;
		console.log("TB Len = " + tbLen);
		console.log(tableList);
		$.each (tableList, function (i, field)
		{
			tbdd.options[i+1] = new Option(field.name, field.name);
		});
	});
}

function getRunTables()
{
	$.getJSON("http://localhost:3000/dm/runtbl", function (runTableList)
	{
		var runTbdd = document.getElementById('runTableDD');
		var runTbLen = runTableList.length;
		console.log("Run TB Len = " + runTbLen);
		console.log(runTableList);
		$.each (runTableList, function (i, field)
		{
			runTbdd.options[i+1] = new Option(field.name, field.name);
		});
	});
}

function getRunCols()
{
	var runTbddVal = document.getElementById('runTableDD').value;
	var def_table = "default." + runTbddVal;
	$.ajax({
		url:'http://localhost:3000/dm/runcl',
		type:'post',
		data:{tb: def_table}
	});
	$.getJSON("http://localhost:3000/dm/runcl", function (runCols)
	{
		dataset = runCols;
		var catColdd = document.getElementById('catColDD');
		var conColdd = document.getElementById('conColDD');
		var colLen = runCols.length;
		var catCount = 0;
		var conCount = 0;
		var keyCount = 6;
		dataset = runCols;
		$.each (runCols, function (i, field)
		{
				console.log(i + " is Cat");
				catColdd.options[catCount] = new Option(field.colm, field.colm);
				catCount++;

				console.log(i + " is Con");
				conColdd.options[conCount] = new Option(Object.keys(field)[keyCount]);
				conCount++;
				keyCount++;

		});
	});
}

function getGraph()
{
	var con = document.getElementById("conColDD").value;
	var cat = document.getElementById("catColDD").value;
	var val = dataset.filter(function(element) { return element.colm === cat; });
	console.log(val[0][con]);
	document.getElementById("image").src = val[0][con];
}

function getGraphInfo()
{
	var cat = document.getElementById("catColDD").value;
	var val = dataset.filter(function(element) { return element.colm === cat; });

	graphData.rows[0].cells[1].innerHTML = val[0]['colm'];
	graphData.rows[1].cells[1].innerHTML = val[0]['col_type'];
	graphData.rows[2].cells[1].innerHTML = val[0]['uniques'];
	graphData.rows[3].cells[1].innerHTML = val[0]['missing'];
	graphData.rows[4].cells[1].innerHTML = val[0]['mean'];
	graphData.rows[5].cells[1].innerHTML = val[0]['stddev'];
}

function dropList()
{
	document.getElementById("conColDD").options.length = 0;
	document.getElementById("catColDD").options.length = 0;
}
