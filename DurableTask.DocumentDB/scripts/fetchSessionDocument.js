function () {
            
    var replacedItem;
    var context = getContext();
    var response = context.getResponse();
    var collection = context.getCollection();

    var query = 'SELECT * FROM c WHERE c.sessionLock = null AND c.orchestrationQueue != null ORDER BY c.orchestratorQueueLastUpdatedTimeUtc ASC'; 
    var accept = collection.queryDocuments(collection.getSelfLink(), query, {},
        function (err, documents, responseOptions) {
            if (err) throw new Error("Error" + err.message);

            if (documents.length == 0) {
                getContext().getResponse().setBody({});
                return;
            }   

            var item = documents[0];
            item.sessionLock = 
            { 
                LockToken : guid(),
                LockedUntilUtc : new Date()
            }
        
            var accept2 = collection.replaceDocument(item._self, item,
                function (err, docReplaced) {
                    if (err) throw "Unable to lock, aborting";
                    response.setBody(item);
                });
    
            if (!accept2) throw "Unable to lock, abort";
    
        });

    if (!accept) throw "Unable get orchestration workitem, abort";
            
    function guid() {
        return s4() + s4() + '-' + s4() + '-' + s4() + '-' +
          s4() + '-' + s4() + s4() + s4();
    }
        
    function s4() {
        return Math.floor((1 + Math.random()) * 0x10000)
          .toString(16)
          .substring(1);
    }
}