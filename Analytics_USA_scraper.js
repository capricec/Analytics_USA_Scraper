// Open up sql connection
var mysql      = require('mysql');
var connection = mysql.createConnection({
  host     : 'localhost',
  user     : 'root',
  password : 'your password here',
  database : 'your database name'
});

connection.connect();

// variables for request / parse function
//create functions which will run and return the correct values as expected
var sql = ["INSERT INTO ALLPAGES (`page`, `page_title`, `visitors`, `date`, `site`) VALUES ?",
           "INSERT INTO COUNTRIES (`country`, `visitors`, `date`, `site`) VALUES ?",
           "INSERT INTO CITIES (`city`, `visitors`, `date`, `site`) VALUES ?",
           "INSERT INTO VISITORS (`visitors`, `date`, `site`) VALUES ?"];

function requests(iteration,i){
  if(iteration == 0){
    console.log("https://analytics.usa.gov/data/" + i + "/all-pages-realtime.csv");
    return "https://analytics.usa.gov/data/" + i + "/all-pages-realtime.csv";
  }
  if(iteration == 1){
    return   "https://analytics.usa.gov/data/" + i + "/top-countries-realtime.json";
  }
  if(iteration == 2){
    return "https://analytics.usa.gov/data/" + i + "/top-cities-realtime.json";
  }
  else{
    return   "https://analytics.usa.gov/data/" + i + "/realtime.json";
  };
};
                
function arraypush(iteration,data, i){ 
  if(iteration == 0){
    return [data.page.substring(0,100), data.page_title.substring(0,100), data.active_visitors, data.dates, i];
  }
  if(iteration == 1){
    return  [data.country, data.active_visitors, data.dates, i];
  }
  if(iteration == 2){
    return [data.city, data.active_visitors, data.dates, i];
  }
  else{
    return  [data.active_visitors, data.dates, i];
  };
};

// function to call either the JSON or the CSV parser based on the file being called
function parsefun(iteration){
  if(iteration == 0){
    return csv.parse({
    	rowDelimiter : '\n',  
    	columns : true, 
      escape : '"'
    });
  }
  else{
    return JSONStream.parse('data.*');
  };
};

var depts = ["live", "ed", "energy", "commerce", "epa", "doi", "doj", "nasa", "nara", "sba", "va"];

var request = require('request');
var csvParse = require('csv-parse');
var csv = require('csv');
var zlib = require('zlib');
var Iconv = require("iconv").Iconv;
var JSONStream = require('JSONStream')

var iteration = 0;
var count = 0; 

//function to call interations on different documents 
function runme(a){
  if(a < sql.length){
  count = 0; 
  check(count);
    }
  else{
  connection.end();
  console.log("end run");
    };
};

//function to call iterations on different department pages
function check(b){
  console.log(b,depts.length);
  if(b < depts.length){
    rdata(depts[b]);
  }
  else{
    iteration ++;
    runme(iteration);    
  };
};

// function to request / parse / write data to sql
function rdata(i){
  console.log(i);
    var datacsv = [];  
    var nIconv = new Iconv("EUC-JP", 'UTF-8//TRANSLIT//IGNORE');
    
      request(requests(iteration, i))
      // Un-Gzip
      .pipe(zlib.createGunzip())
      // // Change Encoding
      .pipe(nIconv)
      // // Parse CSV 
      .pipe(parsefun(iteration))
      .on('error',function(err){
        console.error(err);
      })
      //convert to array of arrays
    	.on('data',function(data){
       //console.log(data);
         data.dates = new Date().toISOString().slice(0, 19).replace('T', ' ');       
         datacsv.push(arraypush(iteration,data, i));
    	})
      //send to database
      .on('end', function(){
          console.log(datacsv);
          connection.query(sql[iteration], [datacsv], function(err) {
              if (err) throw err; });
          count ++;
          console.log('end');
          check(count);
      
    });
};

//rdata("ed");
runme(iteration);



// node ./sqltest4.js
