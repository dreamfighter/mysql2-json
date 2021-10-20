const mysql2json = require('./index');

async function main(){
    const db = await mysql2json({
        host: 'localhost',
        user: 'root',
        password: '',
        database: 'test_db',
        waitForConnections: true,
        connectionLimit: 5,
        queueLimit: 0
    }).khaireen.tables();

    var query = {
        status:'PENDING',
        'orders.no_wait':'N',
        $and:{$or:[{_id:12},{status:'PENDING'},
            {'orders.no_wait':'N'}]}
    }

    db.test_db.find(query).projection({_id:1})
        .join('orders',{},{
            'packing._id':'orders.packing_id',
            'orders.customer_to':12,
            'orders.shipment_id':23
        }).groupBy(['_id'])
        .exec((e,d)=>{
            console.log(d);

        });
}

main()