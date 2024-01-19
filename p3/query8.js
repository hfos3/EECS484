// Query 8
// Find the city average friend count per user using MapReduce.


let city_average_friendcount_mapper = function () {
    if(this.hometown.city != null){
        emit(this.hometown.city, {sum: this.friends.length, count: 1});
    }
 };
 
 
 let city_average_friendcount_reducer = function (key, values) {
    var fCount = 0
    var n = 0
    values.forEach(function(v) {
        n += v.count;
        fCount += v.sum;
    });
    return {sum: fCount,count: n};
 };
 
 
 let city_average_friendcount_finalizer = function (key, reduceVal) {
    // We've implemented a simple forwarding finalize function. This implementation
    // is naive: it just forwards the reduceVal to the output collection.
    // TODO: Feel free to change it if needed.
    if(reduceVal.count == 0){
        return 0;
    }
    return reduceVal.sum / reduceVal.count
 };
 
