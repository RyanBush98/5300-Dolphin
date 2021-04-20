#include "heap_storage.h"
#include "db_cxx.h"
#include <cstring>

using namespace std;
typedef u_int16_t u16

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
    u16 id = this->num_records;
    u16 size = (u16) data->get_size();
    this->end_free -= size;
    u16 loc = this->end_free + 1;
    put_header();
    put_header(id, size, loc);
    memcpy(this->address(loc), data->get_data(), size);
    return id;
}

//gets a record's data
//will have to be unmarshaled
Dbt* SlottedPage::get(RecordID recordID){
    u16 size;
    u16 loc;
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
    u16 size;
    u16 loc;
    get_header(size, loc, recordID);
    put_header(recordID, 0, 0);

}

RecordIDs* SlottedPage::ids(void){
    RecordIDs *result = new RecordIDs;
    u16 size;
    u16 loc;
    for(u16 i = 1; i <= this->num_records;i++){
        if (loc != 0){
            result->push_back(i)                //what is push back?
        }
    }
    return result;
}

//Get the size and offset for given id. For id of zero, it is the block header.
void SlottedPage::get_header(u_int16_t &size, u_int16_t &loc, RecordID id = 0){
    size = get_n((u16)4* id);
    loc = get_n((u16)(4 * id + 2));
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
  u16 capacity;
  capacity = this->end_free -(4 * (this->num_records + 1));
  return size <= capacity;
}

void SlottedPage::slide(u_int16_t start, u_int16_t end){
    u16 shift = end - start;
    if (shift == 0){
        return;
    }

    //do the slide
    memcpy(this->address(this->end_free + 1 + shift), ))         //FIXME

    //move headers to right
    RecordIDs* record_ids = this->ids();
    for(unsigned int i=0; i < record_ids->size(); i++){        
        u16 loc;
        u16 size; 
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
    try{
        this->open();
    }
    catch(DbRelationError){
        this->create();
    }
}

void HeapTable::drop(){
    this->file.drop();
}

void HeapTable::open(){
    this->file.open();
}

void HeapTable::close(){
    this->file.close();
}

Handle HeapTable::insert(const ValueDict *row){
    this->open();
    return this->append(row);
}

Handles* HeapTable::select(const ValueDict *where){
    Handles* handles = new Handles();
    BlockIDs* block_ids = file.block_ids();

    for (const& block_id: *block_ids){
        SlottedPage* block = file.get(block_id);
        RecordIDs* record_ids = block->ids
        for (const& record_id: *record_ids){
            handles->push_back(Handle(block_id, record_id));
        }
        delete block_ids;
        delete block;
    }
    delete block_ids;
    delete handles;
}

Handle HeapTable::append(const ValueDict *row){
    Dbt *data = this->marshal(row);
    SlottedPage *block = this.get(this->file.get_last_block_id());
    u_int16_t record_id;

    try{
        record_id = block->add(data);
    }
    catch(DbRelationError){
        block = this->file.get_new();
        record_id = block->add(data);
    }

    this->file.put(block);
    return Handle(this->file.get_last_block_id, record_id);
}

ValueDict *HeapTable::validate(const ValueDict *row){
    map <Identifier, Value> *full_row = {};
    uint col_num = 0;

    for(auto const &column_name: column_name){
        ColumnAttribute cAtt = this->column_attributes[col_num++];
        ValueDict::const_iterator column = row->find(column_name);
        Value val = column->second;

        if(cAtt.get_data_type() != ColumnAttribute::DataType::INT && cAtt.get_data_type() != ColumnAttribute::DataType::TEXT){
            throw DbRelationError("Not text or int, don't know how to handle NULLs, defaults, etc. yet");
        }
        else{
            val = row->at(column_name);

        }
    full_row[column_name] = val;
    }
    return full_row;
}

//code provided in milestone prompt
Dbt* HeapTable::marshal(const ValueDict *row)
{
    char *bytes = new char[DbBlock::BLOCK_SZ]; // more than we need (we insist that one row fits into DbBlock::BLOCK_SZ)
    uint offset = 0;
    uint col_num = 0;
    for (auto const& column_name: this->column_names) {
        ColumnAttribute ca = this->column_attributes[col_num++];
        ValueDict::const_iterator column = row->find(column_name);
        Value value = column->second;
        if (ca.get_data_type() == ColumnAttribute::DataType::INT) {
            *(int32_t*) (bytes + offset) = value.n;
            offset += sizeof(int32_t);
        } else if (ca.get_data_type() == ColumnAttribute::DataType::TEXT) {
            uint size = value.s.length();
            *(u_int16_t*) (bytes + offset) = size;
            offset += sizeof(u_int16_t);
            memcpy(bytes+offset, value.s.c_str(), size); // assume ascii for now
            offset += size;
        } else {
            throw DbRelationError("Only know how to marshal INT and TEXT");
        }
    }
    char *right_size_bytes = new char[offset];
    memcpy(right_size_bytes, bytes, offset);
    delete[] bytes;
    Dbt *data = new Dbt(right_size_bytes, offset);
    return data;
}

ValueDict *unmarshal(Dbt *data){            //not done!
    uint offset = 0;
    uint col_num = 0;
}