// Query 2
// Unwind friends and create a collection called 'flat_users' where each document has the following schema:
// {
//   user_id:xxx
//   friends:xxx
// }
// Return nothing.

function unwind_friends(dbname) {
    db = db.getSiblingDB(dbname);
    // db.users.find().forEach(function(U){
    //     db.users.aggregate([{$unwind : "$friends"}])
    // });
    //TODO: unwind friends
    db.users.aggregate([
        {
            $unwind : "$friends"
        },
        {
            $project: {
                _id:0,
                user_id:1,
                friends: "$friends"
            }
        },
        {
            $out: "flat_users"
        }
    ]);
    return;
}
