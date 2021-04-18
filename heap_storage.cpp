#include "heap_storage.h"
#include "db_cxx.h"
#include <cstring>

using namespace std;
typedef u_int16_t uint16

bool test_heap_storage() {return true;}


SlottedPage::SlottedPage(Dbt &block, BlockID block_id, bool is_new = false) {
  if (is_new) {
        this->num_records = 0;
        this->end_free = DbBlock::BLOCK_SZ - 1;
        put_header();
  } else {
        get_header(this->num_records, this->end_free);
  }
}

//adds a new record to the block
//assumes record has been marshaled into memory at data
//returnds id for later fetching
RecordID SlottedPage::add(const Dbt *data) {
    if (!has_room(data->getsize())) //write get size
        throw DbBlockNoRoomError("No room for new block")
    uint16 id = this->num_records;
    uint16 size = (uint16) data->get_size();
    this->end_free -= size;
    uint16 loc = this->end_free + 1;
    put_header();
    put_header(id, size, loc);
    memcpy(this->address(loc), data->get_data(), size);
    return id;
}

//gets a record's data
//will have to be unmarshaled
Dbt* SlottedPage::get(RecordID recordID){
    uint16 size;
    uint16 loc;
    get_header(size, loc, recordID);
    //what do if record does not exist?
    if(loc == 0){
        return nullptr;
    }
    return new Dbt(this->address(loc), size);
}

//remove record by id
//set location and id to 0 to show removed
void SlottedPage::delete(RecordID recordID){
    uint16 size;
    uint16 loc;
    get_header(size, loc, recordID);
    put_header(recordID, 0, 0);

}

RecordIDs* SlottedPage::ids(void){
    RecordIDs *result = new RecordIDs;
    uint16 size;
    uint16 loc;
    for(uint16 i = 1; i <= this->num_records;i++){
        if (loc != 0){
            result->push_back(i)                //what is push back?
        }
    }
    return result;
}

//Get the size and offset for given id. For id of zero, it is the block header.
void SlottedPage::get_header(u_int16_t &size, u_int16_t &loc, RecordID id = 0){
    size = get_n((uint16)4* id);
    loc = get_n((uint16)(4 * id + 2));
}

//Put the size and offset for given id. For id of zero, store the block header
void put_header(RecordID id = 0, u_int16_t size = 0, u_int16_t loc = 0){
    if (id == 0){
        size = this->num_records;
        loc = this->end_free;
    }
    put_n(4*id, size);
    put_n(4*id + 2, loc);
}

//
bool SlottedPage::has_room(u_int16_t size) {
  uint16 capacity;
  capacity = this->end_free -(4 * (this->num_records + 1));
  return size <= capacity;
}

void SlottedPage::slide(u_int16_t start, u_int16_t end){
    uint16 shift = end - start;
    if (shift == 0){
        return;
    }

    //do the slide
    memcpy(this->address(this->end_free + 1 + shift), ))         //FIXME

    //move headers to right
    RecordIDs* record_ids = this->ids();
    for(unsigned int i=0; i < record_ids->size(); i++){        
        uint16 loc;
        uint16 size; 
        get_header(size, loc, temp->at(i));
        if (loc <= start){
            loc += shift;
            put_header(temp->at(i), size, loc);
        }
    }
    this->end_free += shift;
    this->put_header();
    delete record_ids;

}


/**
 * ---------------------------Heapfile needs comment here---------------------------
 */

HeapFile::HeapFile(string name) : DbFile(name), dbfilename(""), last(0), closed(true), db(_DB_ENV, 0) {}

//wrapper for berkdb open
void HeapFile::db_open(uint flags) {
	if (!this->closed) 
		return;

	this->db.set_re_len(DbBlock::BLOCK_SZ);
    // get the correct name for db
    // need to test here to make sure it work
	this->dbfilename = this->name + ".db";
	this->db.open(nullptr, (this->dbfilename).c_str(), nullptr, DB_RECNO, flags, 0644);
	DB_BTREE_STAT *stat;
	this->db.stat(nullptr, &stat, DB_FAST_STAT);
	this->last = flags ? 0 : stat->bt_ndata;
	this->closed = false;
}

void HeapFile::create(void) {
    db_open(DB_CREATE | DB_EXCL);
    SlottedPage *block_page = get_new();
    delete block_page;
}

void HeapFile::drop(void){
    close();
    Db db(_DB_ENV, 0);
    db.remove(this->dbfilename.c_str(), nullptr, 0);
}

void HeapFile::open(void){
    db_open();
}

void HeapFile::close(void){
    this->db.close(0);
    this->closed = true;
}

SlottedPage *HeapFile::get_new(void) {
    char block[DbBlock::BLOCK_SZ];
    memset(block, 0, sizeof(block));
    Dbt data(block, sizeof(block));

    int block_id = ++this->last;
    Dbt key(&block_id, sizeof(block_id));

    // mangage memory with berkdb
    SlottedPage *page = new SlottedPage(data, this->last, true);
    this->db.put(nullptr, &key, &data, 0); 
    this->db.get(nullptr, &key, &data, 0);
    return page;
}


/**
 * ---------------------------Heaptable/dbrelation needs comment here---------------------------
 */
HeapTable::HeapTable(Identifier table_name, ColumnNames column_names, ColumnAttributes column_attributes)
{
    DbRelation(table_name, column_names, column_attributes);
    this->file = HeapFile(table_name);
}

void HeapTable::create(){
    this->file.create();
}

void HeapTable::create_if_not_exists(){
    
}
