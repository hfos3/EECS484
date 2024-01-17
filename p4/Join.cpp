#include "Join.hpp"

#include <vector>

using namespace std;

const uint B = MEM_SIZE_IN_PAGE;

//for non full pages after first loop through
void checkPartition(Disk* disk, Mem* mem, bool left,vector<Bucket> &partitions){
	for(uint i = 0; i < B-1; i++){
		Page* p = mem->mem_page(i);
		if(p->empty()){continue;}
		if(left){
			partitions[i].add_left_rel_page(mem->flushToDisk(disk,i));
		}
		else{
			partitions[i].add_right_rel_page(mem->flushToDisk(disk,i));	
		}
	}
}
/*
 * Input: Disk, Memory, Disk page ids for left relation, Disk page ids for right relation
 * Output: Vector of Buckets of size (B - 1) after partition
 */
vector<Bucket> partition(Disk* disk, Mem* mem, pair<uint, uint> left_rel,
                         pair<uint, uint> right_rel) {
			/*
     * Write specific memory page into disk, and reset memory page
     * Return written disk page id
     */
	//uint flushToDisk(Disk* d, uint mem_page_id);
	// TODO: implement partition phase
	vector<Bucket> partitions(B-1, Bucket(disk));

	//since both relations will do identical processes, lambda here to &capture everything easily
	auto partition_individual = [&](pair<uint,uint> rel, bool left){
		for(uint i = rel.first; i < rel.second; i++){
			mem->loadFromDisk(disk,i,B-1);
			Page* p = mem->mem_page(B-1);
			uint n = p->size();
			for(uint j = 0; j < n; j++){
				Record r = p->get_record(j);
				uint h1 = r.partition_hash() % (B-1);
				Page* h_page = mem->mem_page(h1);h_page->loadRecord(r);
				if(h_page->full()){//flush full page to disk, take return value and add to relation
					if(left){
						partitions[h1].add_left_rel_page(mem->flushToDisk(disk,h1));
					}
					else{
						partitions[h1].add_right_rel_page(mem->flushToDisk(disk,h1));
					}
				}
			}
			p->reset();
		}
	};
	partition_individual(left_rel,true);
	checkPartition(disk,mem,true,partitions);
	mem->reset();
	partition_individual(right_rel,false);
	checkPartition(disk,mem,false,partitions);
	mem->reset();
	return partitions;
}

/*
 * Input: Disk, Memory, Vector of Buckets after partition
 * Output: Vector of disk page ids for join result
 */
vector<uint> probe(Disk* disk, Mem* mem, vector<Bucket>& partitions) {
	// TODO: implement probe phase
	vector<uint> disk_pages; 
	for(uint k = 0; k < partitions.size(); k++){
	 	vector<uint> R = partitions[k].get_left_rel();
		vector<uint> S = partitions[k].get_right_rel();
		if(R.size() > S.size()){swap(R,S);}
		uint n = R.size();
		for(uint i = 0; i < n; i++){
			mem->loadFromDisk(disk,R[i],B-2);
			Page* p = mem->mem_page(B-2);
			uint m = p->size();
			for(uint j = 0; j < m; j++){
				Record r = p->get_record(j);
				uint h2 = r.probe_hash() % (B-2);
				mem->mem_page(h2)->loadRecord(r);
			}
			p->reset();
		}
		n = S.size();
		for(uint i = 0; i < n; i++){
			mem->loadFromDisk(disk,S[i],B-2);
			Page* p = mem->mem_page(B-2);
			uint m = p->size();
			for(uint j = 0; j < m; j++){
				Record r = p->get_record(j);
				uint h2 = r.probe_hash() % (B-2);
				Page* h_page = mem->mem_page(h2);
				for(uint h = 0; h < h_page->size(); h++){
					Record h_record = h_page->get_record(h);
					if(h_record == r){
						mem->mem_page(B-1)->loadPair(h_record,r);
						if(mem->mem_page(B-1)->full()){
							disk_pages.push_back(mem->flushToDisk(disk, B - 1));
						}
					}
				}
			}
		}
		for(uint i = 0; i < B - 2; ++i){
			mem->mem_page(i)->reset();
		}
	}
	if(!mem->mem_page(B - 1)->empty()){
		disk_pages.push_back(mem->flushToDisk(disk, B - 1));
	}
	return disk_pages;
}