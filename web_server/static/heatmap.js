var map, pointarray, heatmap;//, respondToMapClick;
var centerofworld = {lat: 0, lng: 0};
var tweetmarkers;

function centerMap(map) {
  var centerControlDiv = document.getElementById('center-button');
  var controlUI = document.getElementById('center-ui');
  controlUI.addEventListener('click', function() {
    map.setCenter(centerofworld);
    map.setZoom(2);
  });
  map.controls[google.maps.ControlPosition.TOP_CENTER].push(centerControlDiv);
}

function setResetPosition(map) {
  var resetPositionDiv = document.getElementById('reset-button');
  var controlUI = document.getElementById('reset-ui');
  controlUI.addEventListener('click', function() {
    response = $('#map').data('tweet');
    renderTweetmap(response);
  });
  map.controls[google.maps.ControlPosition.TOP_CENTER].push(resetPositionDiv);
}

function locationSearch(loc) {
  $.getJSON("location.json/" + loc)
    .done(function(data) {
      response = data;
      var tweets = response.tweets;
      if (tweets.length == 0) {
        alert("No tweets in the vicinity");
      }
      renderTweetmap(response);
    })
    .fail(function(jqxhr, textStatus, error) {
      alert("Failed to get any tweets");
      renderTweetmap(null);
    });
}

function keywordSearch(key) {
  if (key.startsWith('Please')) {
    response = $('#map').data('tweet');
    renderTweetmap(response);
    return;
  }
  $.getJSON("keyword.json/" + key)
    .done(function(data) {
      response = data;
      var tweets = response.tweets;
      if (tweets.length == 0) {
        alert("No tweets matched the selected keyword");
      }
      renderTweetmap(response);
    })
    .fail(function(jqxhr, textStatus, error) {
      alert("No tweets matched the selected keyword");
      renderTweetmap(null);
    });
};

function main() {
  // Map center
  var mapCenter = new google.maps.LatLng(0, 0);

  // Map options
  var mapOptions = {
    zoom: 2,
    center: mapCenter,
    mapTypeId: google.maps.MapTypeId.ROADMAP
  }

  // Render basemap
  map = new google.maps.Map(document.getElementById("map"), mapOptions);
  response = $('#map').data('tweet');
  renderTweetmap(response);
  centerMap(map);
  // map.controls[google.maps.ControlPosition.TOP_CENTER].push(document.getElementById('reset-button'));
  // setupSelectPoint(map);
 
  // Add click listener for geo-location
  google.maps.event.addListener(map, 'click', function (e) {
      var locJSON = "{";
      locJSON += "\"dist\":" + "\"100mi\",";
      locJSON += "\"lat\":" + e.latLng.lat() + ",";
      locJSON += "\"lon\":" + e.latLng.lng();
      locJSON += "}";
      locationSearch(locJSON);
    // if (respondToMapClick) {
    //   var locJSON = "{";
    //   locJSON += "\"dist\":" + "\"100mi\",";
    //   locJSON += "\"lat\":" + e.latLng.lat() + ",";
    //   locJSON += "\"lon\":" + e.latLng.lng();
    //   locJSON += "}";
    //   locationSearch(locJSON);
    // }
    // respondToMapClick = false;
  });

  setResetPosition(map);

  $( ".datepicker" ).change(function() {
    keywordSearch($( ".datepicker option:selected" ).text());
  });
}

function renderTweetmap(response) {

  // Remove all markers first
  if ( tweetmarkers != null && tweetmarkers.length > 0 ) {
    for (var j = 0; j < tweetmarkers.length; j++) {
      tweetmarkers[j].setMap(null);
    }
  }

  var iconBase = 'static/';

  //var data = [];
  tweetmarkers = [];
  // Transform data format
  var tweets = response.tweets;
  for (i in tweets) {
    //data.push(new google.maps.LatLng(parseFloat(tweets[i].lat), parseFloat(tweets[i].lon)));
    // var sent = 'neutral';
    // if ( tweets[i].sentiment == 'positive' ) {
    //   sent = 'smile';
    // } else if ( tweets[i].sentiment == 'negative' ) {
    //   sent = 'frown';
    // }

    var marker = new google.maps.Marker({
      position: {lat: tweets[i].lat, lng: tweets[i].lon},
      map: map,
      title: tweets[i].text,
      icon: iconBase + tweets[i].sentiment + '.png'
    });

    tweetmarkers.push(marker);

  }

  // var pointArray = new google.maps.MVCArray(data);

  // // Create heatmap
  // heatmap = new google.maps.visualization.HeatmapLayer({
  //   data: pointArray
  // });
  // heatmap.set('radius', heatmap.get('radius') ? null : 15);
  // heatmap.set('opacity', heatmap.get('opacity') ? null : .75);
  // heatmap.setMap(map);
}

window.onload = main;