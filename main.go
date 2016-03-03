package main
import (
	"fmt"
	"time"
	"gopkg.in/mgo.v2"
	"strconv"
	"github.com/spf13/viper"
)

type Config struct {
	Addrs string
	Timeout int
	Database string
	Username string
	Password string
}
var CFG Config

var count int = 0
var notFound int = 0
var DB *mgo.Session

var start chan bool
var die chan bool

var str string = "Test String Small length"


func createIndex() error{
	var index mgo.Index
	index = mgo.Index{
		Key:[]string{"value"},
	}
	err := DB.DB("test").C("test").EnsureIndex(index)
	if err!= nil{
		return err
	}

	for i:=1; i<15; i++{
		index = mgo.Index{
			Key:[]string{"value"+strconv.Itoa(i)},
		}

		err = DB.DB("test").C("test").EnsureIndex(index)
		if err!= nil{
			return err
		}
	}

	return nil
}

func threadWrite (i int) {

	d:= DB.Copy()
	defer d.Close()
	arr:= make(map[string]interface{})

	<-start
	for{
		select {
		case <-die:
			return
			default:
				count++
				arr["value"] = str
				arr["value1"] = count + 1
				arr["value2"] = count + 2
				arr["value3"] = count + 3
				arr["value4"] = count + 4
				arr["value5"] = count + 5
				arr["value6"] = count + 6
				arr["value7"] = count + 7
				arr["value8"] = count + 8
				arr["value9"] = count + 9
				arr["value10"] = count + 10
				arr["value11"] = count + 11
				arr["value12"] = count + 12
				arr["value13"] = count + 13
				arr["value14"] = count + 14

				d.DB("test").C("test").Insert(arr)

		}
	}
}
func threadRead (i int) {
	d:= DB.Copy()
	defer d.Close()

	arr:= make(map[string]interface{})

	<-start
	for{
		select {
		case <-die:
			return
		default:
			count++
			arr["value"] = str
			arr["value1"] = count + 1
			arr["value2"] = count + 2
			arr["value3"] = count + 3
			arr["value4"] = count + 4
			arr["value5"] = count + 5
			arr["value6"] = count + 6
			arr["value7"] = count + 7
			arr["value8"] = count + 8
			arr["value9"] = count + 9
			arr["value10"] = count + 10
			arr["value11"] = count + 11
			arr["value12"] = count + 12
			arr["value13"] = count + 13
			arr["value14"] = count + 14


			var res interface{}
			err := d.DB("test").C("test").Find(arr).One(&res)
			if err != nil {
				if err == mgo.ErrNotFound {
					notFound++
				} else {
					fmt.Println(err)
				}
			}

		}
	}
}

func main(){
	fmt.Println("Start tester")
	viper.SetConfigName("config")
	viper.AddConfigPath(".")
	err := viper.ReadInConfig()
	if err != nil {
		fmt.Println(err)
	}
	err =viper.Unmarshal(&CFG)
	if err != nil {
		fmt.Println(err)
	}


	start = make(chan bool,2)
	die = make(chan bool,2)

	seconds := 1
	namberGorutines:= 10

	info := &mgo.DialInfo{
		Addrs:    []string{CFG.Addrs},
		Timeout:  time.Duration(CFG.Timeout) * time.Second,
		Database: CFG.Database,
		Username: CFG.Username,
		Password: CFG.Password,
	}

	fmt.Println("Connect to database")
	db, err := mgo.DialWithInfo(info)
	if err != nil {
		fmt.Println(err)
	}
	fmt.Println("Connecting success")

	db.SetMode(mgo.Monotonic, true)
	DB = db
	DB.DB("test").C("test").DropCollection()

	/* ============ Тест записи без индексов ============  */
	for i:=0; i<namberGorutines; i++{
		go threadWrite(i)
	}

	for i:=0; i<namberGorutines; i++{
		start <- true
	}

	time.Sleep(time.Duration(seconds) * time.Second)
	for i:=0; i<namberGorutines; i++{
		die <-true
	}
	time.Sleep(1 * time.Second)

	fmt.Println("Write:                                   ", count/seconds,"TPS")

	/* ============  Тест чтения без индексов ============  */
	count = 0
	for i:=0; i<namberGorutines; i++{
		go threadRead(i)
	}

	for i:=0; i<namberGorutines; i++{
		start <- true
	}

	time.Sleep(time.Duration(seconds) * time.Second)
	for i:=0; i<namberGorutines; i++{
		die <-true
	}
	time.Sleep(1 * time.Second)

	fmt.Println("Read:                                    ", count/seconds, "TPS", "          Not Found: ", notFound)

	/* ============ Тест записи с единым индексом ============ */
	count = 0
	indexG := mgo.Index{
		Key: []string{"value","value1","value2","value3","value4","value5","value6","value7","value8","value9","value10","value11","value12","value13","value14",},
		Name: "all",
	}
	err = DB.DB("test").C("test").EnsureIndex(indexG)
	if err != nil{
		fmt.Println(err)
	}

	for i:=0; i<namberGorutines; i++{
		go threadWrite(i)
	}

	for i:=0; i<namberGorutines; i++{
		start <- true
	}

	time.Sleep(time.Duration(seconds) * time.Second)
	for i:=0; i<namberGorutines; i++{
		die <-true
	}
	time.Sleep(1 * time.Second)

	fmt.Println("Write with global index:                 ", count/seconds,"TPS")

	/* ============ Тест чтения с единым индексом ============ */
	count = 0
	for i:=0; i<namberGorutines; i++{
		go threadRead(i)
	}

	for i:=0; i<namberGorutines; i++{
		start <- true
	}

	time.Sleep(time.Duration(seconds) * time.Second)
	for i:=0; i<namberGorutines; i++{
		die <-true
	}
	time.Sleep(1 * time.Second)

	fmt.Println("Read with global index:                  ", count/seconds,"TPS", "          Not Found: ", notFound)



	/* ============ Тест записи с индексом для каждого поля ============ */
	count = 0
	err = DB.DB("test").C("test").DropIndexName("all")
	if err!=nil{
		fmt.Println(err)
	}
	err = createIndex()
	if err != nil {
		fmt.Println(err)
	}

	for i:=0; i<namberGorutines; i++{
		go threadWrite(i)
	}

	for i:=0; i<namberGorutines; i++{
		start <- true
	}

	time.Sleep(time.Duration(seconds) * time.Second)
	for i:=0; i<namberGorutines; i++{
		die <-true
	}
	time.Sleep(1 * time.Second)

	fmt.Println("Write with index for each field:         ", count/seconds,"TPS")

	/* ============ Тест чтения с индексом для каждого поля ============ */

	count = 0
	for i:=0; i<namberGorutines; i++{
		go threadRead(i)
	}

	for i:=0; i<namberGorutines; i++{
		start <- true
	}

	time.Sleep(time.Duration(seconds) * time.Second)
	for i:=0; i<namberGorutines; i++{
		die <-true
	}
	time.Sleep(1 * time.Second)

	fmt.Println("Read with indexes for each field:        ", count/seconds,"TPS", "          Not Found: ", notFound)
}