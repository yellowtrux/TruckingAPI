/* SHIPMENT & OFFER SYSTEM JSON API */

package main

import (
    "database/sql"
    "encoding/json"
    "fmt"
    "log"
    "net/http"
    "os" 

    "github.com/gorilla/mux"
    _ "github.com/mattn/go-sqlite3"
)

type Driver struct {
    DriverID    int64   `json:"driverid"`
    Name        string  `json:"name"`
    Capacity    int     `json:"capacity"`
    NoOfOffers  int     `json:"noffers"`
}

type Shipment struct {
    ShipmentID  int64   `json:"shipmentid"`
    Title       string  `json:"title"`
    Capacity    int     `json:"capacity"`
    Status      int     `json:"status"`
}

type Offer struct {
    OfferID     int64   `json:"offerid"`
    ShipmentID  int64   `json:"shipmentid"`
    DriverID    int64   `json:"driverid"`
    Status      int     `json:"status"`
}

//Status, in db tables 
//these start from 0
const (
    OfferActive = iota     
    OfferAccepted   
    OfferPassed
    OfferRevoked      
)

const (
    ShipPending = iota
    ShipOffersReady
    ShipPendingAccept
    ShipAccepted
    ShipInProgress
    ShipComplete
)

var db *sql.DB              // global for database access from all endpoints
   
//CreateDriver == CreateTruckDriver 
//@TODO in the real world, there'd be a many to many mapping between drivers and trucks
//Adds a new driver to the system.  We will assume all drivers have one truck and each driver has 
//a maximum capacity (weight). Each shipment also has a required capacity and if a shipment's capacity 
//exceeds a truck's maximum capacity, then we should not offer the job to that driver.
//**Inputs**
//* _capacity_: The capacity the driver's truck can carry.
//
//**Outputs**
//* _id_: A unique identifier for the new driver

func CreateDriver(w http.ResponseWriter, req *http.Request){    
    var driver Driver
    params   := mux.Vars(req)
    name     := params["name"]
    capacity := params["capacity"]

    fmt.Printf("CreateDriver: name=%s\n", name)
    fmt.Printf("CreateDriver: capacity=%s\n", capacity)

    stmt, err := db.Prepare("INSERT INTO drivers(name, capacity, noffers) values (?,?,?)")
    checkErr(err)
    _, err = stmt.Exec(name, capacity, 0)  //init no of offers to 0
    checkErr(err)
    stmt.Close()

    row, err := db.Query("SELECT driverid, name, capacity, noffers FROM drivers WHERE name = ?", name)
    checkErr(err)
    row.Next()
    row.Scan(&driver.DriverID, &driver.Name, &driver.Capacity, &driver.NoOfOffers)
    row.Close()
    json.NewEncoder(w).Encode(driver)
}

//CreateShipment
//Create a new shipment and create offers to the top 10 eligible drivers.  
//A driver is eligible if they have a truck that can carry the required _capacity_ for the shipment.  
//To be fair to drivers, drivers should be sorted by how many offers they have received in the past.  
//Drivers who have had less opportunity to accept jobs are put on the top.
//**Inputs**
//* _capacity_: The capacity required for the shipment

//**Outputs**
//* _id_: A unique identifier for the new shipment  
//* _offers_: An array of offers that was created for the shipment

func CreateShipment(w http.ResponseWriter, req *http.Request){    
    var shipID     int64
    var shipment   Shipment
    var offer      Offer
    var offers     []Offer

    params   := mux.Vars(req)
    title    := params["title"]
    capacity := params["capacity"]

    fmt.Printf("CreateShipment: title=%s\n", title)
    fmt.Printf("CreateShipment: capacity=%s\n", capacity)

    stmt, err := db.Prepare("INSERT INTO shipments(title, capacity, status) values (?,?,?)")
    checkErr(err)
    _, err = stmt.Exec(title, capacity, ShipPending)  
    checkErr(err)
    stmt.Close()

    //@TODO get ShipID for this title based on last insert
    //and check for duplicate titles combined with date or some not yet added column
    //query below doesn't work -- wrong name from shipments table, I guess
    //err = db.QueryRow(`SELECT seq FROM sqlite_sequence WHERE name=shipments`).Scan(shipmentID)

    row, err := db.Query("SELECT shipmentid FROM shipments WHERE title = ?", title)
    checkErr(err)
    row.Next()
    row.Scan(&shipID) 
    row.Close()

    fmt.Printf("CreateShipment: new shipment.ShipmentID=%s\n", shipID)

    //find 10 drivers-trucks with required capacity and lowest number of offers
    rows, err := db.Query(`SELECT   driverid 
                           FROM     drivers 
                           WHERE    capacity >= ? 
                           ORDER BY noffers ASC
                           LIMIT 10`, capacity)
    checkErr(err)
    for rows.Next() {
        offer.OfferID = 0
        offer.ShipmentID = shipID
        rows.Scan(&offer.DriverID)
        offer.Status = OfferActive
        offers = append(offers, offer)
    }
    rows.Close()

    //increment noffers (number of offers) for this set of drivers
    for _, offer = range offers {
        stmt, err := db.Prepare(`UPDATE drivers SET noffers=(noffers + 1) WHERE driverid=?`)
        checkErr(err)
        _, err = stmt.Exec(offer.DriverID)  
        checkErr(err)
    }
    stmt.Close()
    
    //create all the offers
    for _, offer = range offers {
        stmt, err = db.Prepare(`INSERT INTO offers(shipmentid, driverid, status) values (?,?,?)`)
        checkErr(err)
        _, err = stmt.Exec(offer.ShipmentID, offer.DriverID, offer.Status)  
        checkErr(err)    
    }
    stmt.Close()

    //all offers have been extended, so set shipment.Status to ShipOffersReady
    stmt, err = db.Prepare(`UPDATE shipments SET status=? WHERE shipmentid=?`)
    checkErr(err)
    _, err = stmt.Exec(ShipOffersReady, shipID)
    checkErr(err)
    stmt.Close()

    row, err = db.Query(`SELECT * FROM shipments WHERE shipmentid = ?`, shipID)
    row.Next()
    row.Scan(&shipment.ShipmentID, &shipment.Title, &shipment.Capacity, &shipment.Status)
    checkErr(err)
    row.Close()

    offers = nil //clear out offers slice and reuse
    rows, err = db.Query(`SELECT * FROM offers WHERE shipmentid = ?`, shipID)
    checkErr(err)
    for rows.Next() {
        rows.Scan(&offer.OfferID, &offer.ShipmentID, &offer.DriverID, &offer.Status)
        offers = append(offers,offer)
    }
    rows.Close()
    json.NewEncoder(w).Encode(shipment)
    json.NewEncoder(w).Encode(offers)
}

//### `GET /shipment/<shipmentId>`
//If no driver has accepted the shipment, returns all outstanding offers for the shipment.
//If shipment has been accepted, returns the accepted offer.
func GetShipment(w http.ResponseWriter, req *http.Request) {
    var status   int
    var accepted bool
    var offer Offer
    var offers []Offer

    params     := mux.Vars(req)
    shipmentid := params["shipmentid"]

    fmt.Printf("GetShipment, shipmentid = %s\n", shipmentid)

    row, err := db.Query("SELECT status FROM shipments WHERE shipmentid=?", shipmentid)
    checkErr(err)
    row.Next()
    row.Scan(&status)
    if status == ShipAccepted {
        accepted = true 
        status = OfferAccepted
    } else {
        accepted = false
        status = OfferActive
    }
    row.Close()
    rows, err := db.Query("SELECT * FROM offers WHERE shipmentid=? AND status=? ", shipmentid, status)
    checkErr(err)
    for rows.Next() {
        rows.Scan(&offer.OfferID, &offer.ShipmentID, &offer.DriverID, &offer.Status)
        offers = append(offers,offer)
    }
    rows.Close()
    json.NewEncoder(w).Encode(accepted)
    json.NewEncoder(w).Encode(offers)    

}

func GetOffersByDriver(w http.ResponseWriter, req *http.Request) {
    var offer  Offer
    var offers []Offer

    params   := mux.Vars(req)
    driverid := params["driverid"]
    
    fmt.Printf("GetOffersByDriver, driverid = %s\n", driverid)

    // for this driver, get the offers marked Active where the shipment status is ShipOffersReady
    rows, err := db.Query(`SELECT o.offerid, o.shipmentid, o.driverid, o.status
                           FROM offers AS o
                           INNER JOIN shipments AS s
                           ON o.shipmentid = s.shipmentid
                           WHERE o.status=? AND s.status=? AND o.driverid=?`, 
                           OfferActive, ShipOffersReady, driverid)
    checkErr(err)
    for rows.Next() {
        rows.Scan(&offer.OfferID, &offer.ShipmentID, &offer.DriverID, &offer.Status)
        offers = append(offers, offer)
    }
    rows.Close()
 
    json.NewEncoder(w).Encode(offers)
}

//### `PUT /offer/<offerId>`
//Accept or reject an offer.  
//If an offer is accepted, it should revoke all other offers for the same shipment.  
//If the offer is rejected, it should update the offer so that it no longer shows up as an active offer 
//for the shipment and driver.
//**Inputs**
//* _status_: `ACCEPT` if the driver accepts the offer or `PASS` if the driver rejects the offer.
//**Outputs**
//None
func AcceptOrRejectOffer(w http.ResponseWriter, req *http.Request) {
    var shipmentid int64
    params  := mux.Vars(req)
    offerid := params["offerid"]
    action  := params["action"]

    fmt.Printf("AcceptOrRejectOffer, offerid = %s\n", offerid)
    fmt.Printf("AcceptOrRejectOffer, action = %s\n", action)

    //I'm assuming that only offerid's with offer.status == OfferActive and 
    //associated shipment.status == ShipOffersReady will be used as input, but
    //this may be an incorrect assumption

    switch action {

    case `PASS` : 
        stmt, err := db.Prepare("UPDATE offers SET status=? WHERE offerid=?")
        checkErr(err)
        _, err1 := stmt.Exec(OfferPassed, offerid)
        checkErr(err1)
        stmt.Close()

    case `ACCEPT` :     
        //find the shipment associated with this offer
        row, err := db.Query(`SELECT shipmentid FROM offers WHERE offerid=?`, offerid)
        checkErr(err)
        row.Next()
        row.Scan(&shipmentid)
        row.Close()

        //Set status to ShipPendingAccept until all offers are revoked
        stmt, err := db.Prepare(`UPDATE shipments SET status=? WHERE shipmentid=?`)
        checkErr(err)
        _, err = stmt.Exec(ShipPendingAccept, shipmentid)
        checkErr(err)
        stmt.Close()
        
        //First revoke all the offers
        stmt, err = db.Prepare(`UPDATE offers SET status=? WHERE shipmentid=?`)
        checkErr(err)
        _, err = stmt.Exec(OfferRevoked, shipmentid)
        checkErr(err)
        stmt.Close()
        
        //Then set this offer to accepted
        stmt, err = db.Prepare(`UPDATE offers SET status=? WHERE offerid=?`)
        checkErr(err)
        _, err = stmt.Exec(OfferAccepted, offerid)
        checkErr(err)
        stmt.Close()
        
        //Now set shipment.Status to ShipAccepted
        stmt, err = db.Prepare(`UPDATE shipments SET status=? WHERE shipmentid=?`)
        checkErr(err)
        _, err = stmt.Exec(ShipAccepted, shipmentid)
        checkErr(err)
        stmt.Close()
    }
}

func GetAllDrivers(w http.ResponseWriter, req *http.Request) {
    var driver Driver
    var drivers []Driver

    rows, err := db.Query("SELECT * FROM drivers")
    checkErr(err)
    for rows.Next() {
        rows.Scan(&driver.DriverID, &driver.Name, &driver.Capacity, &driver.NoOfOffers)
        drivers = append(drivers, driver)
    }
    rows.Close()
    json.NewEncoder(w).Encode(drivers)
}

func GetAllShipments(w http.ResponseWriter, req *http.Request) {
    var shipment Shipment
    var shipments []Shipment

    rows, err := db.Query("SELECT * FROM shipments")
    checkErr(err)
    for rows.Next() {
        rows.Scan(&shipment.ShipmentID, &shipment.Title, &shipment.Capacity, &shipment.Status)
        shipments = append(shipments, shipment)
    }
    rows.Close()
    json.NewEncoder(w).Encode(shipments)
}

func GetAllOffers(w http.ResponseWriter, req *http.Request) {
    var offer Offer
    var offers []Offer

    rows, err := db.Query("SELECT * FROM offers")
    checkErr(err)
    for rows.Next() {
        rows.Scan(&offer.OfferID, &offer.ShipmentID, &offer.DriverID, &offer.Status)
        offers = append(offers, offer)
    }
    rows.Close()
    json.NewEncoder(w).Encode(offers)
}

func checkErr(err error) {
    if err != nil {
        log.Printf("%s\n", err)
        //sendErrorResponse(w,err,http.StatusBadRequest)
        fmt.Printf("%s\n", err)
        //panic(err) 
        //perhaps find a way to recover from panic? 
    }
}

func main() {

    os.Remove("./convoyapi.db")

    var err error
    db, err = sql.Open("sqlite3", "./convoyapi.db")
    if err != nil {
        log.Fatal(err)
    }
    defer db.Close()

    // Create drivers table
    sqlStmt := `
    create table drivers (driverid integer primary key autoincrement, 
                          name text, 
                          capacity int, 
                          noffers int);
    delete from drivers;
    `
    _, err = db.Exec(sqlStmt)
    if err != nil {
        log.Printf("%q: %s\n", err, sqlStmt)
        fmt.Printf("%q: %s\n", err, sqlStmt)
        return
    } 

    // Create shipments table
    sqlStmt = `
    create table shipments (shipmentid integer primary key autoincrement, 
                            title text, 
                            capacity integer, 
                            status integer);
    delete from shipments;
    `
    _, err = db.Exec(sqlStmt)
    if err != nil {
        log.Printf("%q: %s\n", err, sqlStmt)
        fmt.Printf("%q: %s\n", err, sqlStmt)
        return
    }


    // Create offers table
    sqlStmt = `
    create table offers (offerid integer primary key autoincrement, 
                         shipmentid integer,
                         driverid integer,  
                         status integer);
    delete from offers;
    `
    _, err = db.Exec(sqlStmt)
    if err != nil {
        log.Printf("%q: %s\n", err, sqlStmt)
        fmt.Printf("%q: %s\n", err, sqlStmt)
        return
    }

    // Insert some drivers to get started
    /*
    stmt, err := db.Prepare("INSERT INTO drivers(name, capacity, noffers) values (?,?,?)")
    checkErr(err)
    _, err = stmt.Exec("Flatbed Annie", 42, 0)
    _, err = stmt.Exec("Rubber Duck", 54, 0)
    _, err = stmt.Exec("Pig Pen", 84, 0)
    _, err = stmt.Exec("Spider Mike", 99, 0)
    _, err = stmt.Exec("Sweetie Pie", 108, 0)
    _, err = stmt.Exec("Broken Bunny", 189, 0)
    _, err = stmt.Exec("Trout Stalker", 216, 0)
    _, err = stmt.Exec("Road Hog", 240, 0)
    _, err = stmt.Exec("Scrap King", 250, 0)
    _, err = stmt.Exec("Telecaster", 300, 0)
    _, err = stmt.Exec("Eleanor Rigby", 350, 0)
    checkErr(err)
    stmt.Close()
    */

    router := mux.NewRouter()
    router.HandleFunc("/driver/{name}/{capacity}", CreateDriver).Methods("POST")
    router.HandleFunc("/shipment/{title}/{capacity}", CreateShipment).Methods("POST")
    router.HandleFunc("/shipment/{shipmentid}", GetShipment).Methods("GET")
    router.HandleFunc("/driver/{driverid}", GetOffersByDriver).Methods("GET")
    router.HandleFunc("/offer/{offerid}/{action}", AcceptOrRejectOffer).Methods("PUT")

    //endpoints for debugging
    router.HandleFunc("/GetAllDrivers", GetAllDrivers).Methods("GET")
    router.HandleFunc("/GetAllShipments", GetAllShipments).Methods("GET")
    router.HandleFunc("/GetAllOffers", GetAllOffers).Methods("GET")
    
    log.Fatal(http.ListenAndServe(":8080", router))
}



