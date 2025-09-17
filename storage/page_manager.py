"""
页式存储管理器
实现页的分配、释放、读写操作
每页大小固定为4KB，页编号唯一
"""
import os
import struct
from typing import Dict, List, Optional, Tuple
from dataclasses import dataclass
from enum import Enum


class PageState(Enum):
    """页状态"""
    FREE = "FREE"
    ALLOCATED = "ALLOCATED"
    DIRTY = "DIRTY"


@dataclass
class PageHeader:
    """页头信息"""
    page_id: int
    page_type: str  # "data", "catalog", "free"
    table_name: str  # 所属表名
    record_count: int
    free_space: int
    next_page: int  # 链表中的下一页


class Page:
    """页类，表示一个4KB的存储页"""
    
    PAGE_SIZE = 4096  # 4KB
    HEADER_SIZE = 80  # 页头大小 (4+32+32+4+4+4=80字节)
    
    def __init__(self, page_id: int):
        self.page_id = page_id
        self.header = PageHeader(
            page_id=page_id,
            page_type="free",
            table_name="",
            record_count=0,
            free_space=Page.PAGE_SIZE - Page.HEADER_SIZE,
            next_page=-1
        )
        self.data = bytearray(Page.PAGE_SIZE)
        self.is_dirty = False
        self.state = PageState.FREE
    
    def serialize_header(self) -> bytes:
        """序列化页头"""
        return struct.pack(
            ">I32s32sIIi",  # 大端序，I=int, 32s=32字节字符串, i=int
            self.header.page_id,
            self.header.page_type.encode('utf-8').ljust(32, b'\x00'),
            self.header.table_name.encode('utf-8').ljust(32, b'\x00'),
            self.header.record_count,
            self.header.free_space,
            self.header.next_page
        )
    
    def deserialize_header(self, data: bytes):
        """反序列化页头"""
        if len(data) < Page.HEADER_SIZE:
            raise ValueError("页头数据不足")
        
        unpacked = struct.unpack(">I32s32sIIi", data[:Page.HEADER_SIZE])
        self.header.page_id = unpacked[0]
        self.header.page_type = unpacked[1].decode('utf-8').rstrip('\x00')
        self.header.table_name = unpacked[2].decode('utf-8').rstrip('\x00')
        self.header.record_count = unpacked[3]
        self.header.free_space = unpacked[4]
        self.header.next_page = unpacked[5]
    
    def write_data(self, offset: int, data: bytes) -> bool:
        """向页中写入数据"""
        if offset + len(data) > Page.PAGE_SIZE:
            return False
        
        self.data[offset:offset + len(data)] = data
        self.is_dirty = True
        return True
    
    def read_data(self, offset: int, length: int) -> bytes:
        """从页中读取数据"""
        if offset + length > Page.PAGE_SIZE:
            return b''
        
        return bytes(self.data[offset:offset + length])
    
    def get_free_space(self) -> int:
        """获取可用空间"""
        return self.header.free_space
    
    def allocate_space(self, size: int) -> Optional[int]:
        """分配空间，返回偏移量"""
        if size > self.header.free_space:
            return None
        
        offset = Page.PAGE_SIZE - self.header.free_space
        self.header.free_space -= size
        self.header.record_count += 1
        self.is_dirty = True
        
        return offset
    
    def to_bytes(self) -> bytes:
        """将页转换为字节数组"""
        header_bytes = self.serialize_header()
        self.data[:Page.HEADER_SIZE] = header_bytes
        return bytes(self.data)
    
    def from_bytes(self, data: bytes):
        """从字节数组加载页"""
        if len(data) != Page.PAGE_SIZE:
            raise ValueError(f"页大小错误: 期望{Page.PAGE_SIZE}字节，实际{len(data)}字节")
        
        self.data = bytearray(data)
        self.deserialize_header(data)


class PageManager:
    """页管理器"""
    
    def __init__(self, data_file: str = "database.dat"):
        self.data_file = data_file
        self.pages: Dict[int, Page] = {}
        self.free_pages: List[int] = []
        self.next_page_id = 0
        
        # 初始化或加载数据文件
        self._initialize_storage()
    
    def _initialize_storage(self):
        """初始化存储系统"""
        if os.path.exists(self.data_file):
            self._load_from_file()
        else:
            self._create_new_file()
    
    def _create_new_file(self):
        """创建新的数据文件"""
        # 创建元数据页
        meta_page = Page(0)
        meta_page.header.page_type = "meta"
        meta_page.header.record_count = 0
        meta_page.header.free_space = Page.PAGE_SIZE - Page.HEADER_SIZE
        self.pages[0] = meta_page
        self.next_page_id = 1
        
        # 写入文件
        self._write_page_to_file(meta_page)
    
    def _load_from_file(self):
        """从文件加载页信息"""
        try:
            with open(self.data_file, 'rb') as f:
                # 读取元数据页
                meta_data = f.read(Page.PAGE_SIZE)
                if len(meta_data) == Page.PAGE_SIZE:
                    meta_page = Page(0)
                    meta_page.from_bytes(meta_data)
                    self.pages[0] = meta_page
                    self.next_page_id = 1
                
                # 读取其他页
                page_id = 1
                while True:
                    page_data = f.read(Page.PAGE_SIZE)
                    if len(page_data) == 0:
                        break
                    if len(page_data) == Page.PAGE_SIZE:
                        page = Page(page_id)
                        page.from_bytes(page_data)
                        self.pages[page_id] = page
                        
                        if page.header.page_type == "free":
                            self.free_pages.append(page_id)
                        
                        page_id += 1
                
                self.next_page_id = page_id
        except Exception as e:
            print(f"加载数据文件失败: {e}")
            self._create_new_file()
    
    def _write_page_to_file(self, page: Page):
        """将页写入文件"""
        try:
            with open(self.data_file, 'r+b') as f:
                f.seek(page.page_id * Page.PAGE_SIZE)
                f.write(page.to_bytes())
        except FileNotFoundError:
            # 文件不存在，创建新文件
            with open(self.data_file, 'wb') as f:
                f.write(page.to_bytes())
    
    def allocate_page(self, page_type: str = "data", table_name: str = "") -> int:
        """分配新页"""
        page_id = self.next_page_id
        page = Page(page_id)
        page.header.page_type = page_type
        page.header.table_name = table_name
        page.state = PageState.ALLOCATED
        
        self.pages[page_id] = page
        self.next_page_id += 1
        
        # 写入文件
        self._write_page_to_file(page)
        
        return page_id
    
    def free_page(self, page_id: int) -> bool:
        """释放页"""
        if page_id not in self.pages:
            return False
        
        page = self.pages[page_id]
        page.header.page_type = "free"
        page.header.table_name = ""
        page.header.record_count = 0
        page.header.free_space = Page.PAGE_SIZE - Page.HEADER_SIZE
        page.state = PageState.FREE
        page.is_dirty = True
        
        self.free_pages.append(page_id)
        
        # 写入文件
        self._write_page_to_file(page)
        
        return True
    
    def read_page(self, page_id: int) -> Optional[Page]:
        """读取页"""
        if page_id in self.pages:
            return self.pages[page_id]
        
        # 从文件读取
        try:
            with open(self.data_file, 'rb') as f:
                f.seek(page_id * Page.PAGE_SIZE)
                page_data = f.read(Page.PAGE_SIZE)
                
                if len(page_data) == Page.PAGE_SIZE:
                    page = Page(page_id)
                    page.from_bytes(page_data)
                    self.pages[page_id] = page
                    return page
        except Exception as e:
            print(f"读取页{page_id}失败: {e}")
        
        return None
    
    def write_page(self, page_id: int, page: Page) -> bool:
        """写入页"""
        try:
            self.pages[page_id] = page
            self._write_page_to_file(page)
            return True
        except Exception as e:
            print(f"写入页{page_id}失败: {e}")
            return False
    
    def flush_all(self):
        """刷新所有脏页到磁盘"""
        for page_id, page in self.pages.items():
            if page.is_dirty:
                self._write_page_to_file(page)
                page.is_dirty = False
        
        # 更新文件头中的页数信息
        self._update_file_header()
    
    def _update_file_header(self):
        """更新文件头中的页数信息"""
        try:
            with open(self.data_file, 'r+b') as f:
                # 读取现有文件头
                f.seek(0)
                header_data = f.read(16)
                if len(header_data) < 16:
                    return
                
                # 更新页数信息
                page_count = len(self.pages)
                header_data = header_data[:8] + page_count.to_bytes(8, byteorder='little')
                
                # 写回文件头
                f.seek(0)
                f.write(header_data)
                f.flush()
                
                print(f"更新文件头：页数 = {page_count}")
        except Exception as e:
            print(f"更新文件头失败: {e}")
    
    def get_page_count(self) -> int:
        """获取页总数"""
        return len(self.pages)
    
    def get_free_page_count(self) -> int:
        """获取空闲页数量"""
        return len(self.free_pages)
    
    def get_page_info(self, page_id: int) -> Optional[Dict]:
        """获取页信息"""
        if page_id not in self.pages:
            return None
        
        page = self.pages[page_id]
        return {
            'page_id': page.page_id,
            'page_type': page.header.page_type,
            'record_count': page.header.record_count,
            'free_space': page.header.free_space,
            'next_page': page.header.next_page,
            'is_dirty': page.is_dirty,
            'state': page.state.value
        }


def main():
    """测试页管理器"""
    # 创建页管理器
    pm = PageManager("test_database.dat")
    
    print("页管理器测试:")
    print(f"总页数: {pm.get_page_count()}")
    print(f"空闲页数: {pm.get_free_page_count()}")
    
    # 分配几个页
    page1_id = pm.allocate_page("data")
    page2_id = pm.allocate_page("catalog")
    
    print(f"分配页: {page1_id}, {page2_id}")
    print(f"总页数: {pm.get_page_count()}")
    
    # 读取页
    page1 = pm.read_page(page1_id)
    if page1:
        print(f"页{page1_id}信息: {pm.get_page_info(page1_id)}")
        
        # 写入一些数据
        test_data = b"Hello, World!"
        offset = page1.allocate_space(len(test_data))
        if offset is not None:
            page1.write_data(offset, test_data)
            pm.write_page(page1_id, page1)
            print(f"在页{page1_id}偏移{offset}写入数据")
    
    # 释放页
    pm.free_page(page1_id)
    print(f"释放页{page1_id}")
    print(f"空闲页数: {pm.get_free_page_count()}")
    
    # 刷新到磁盘
    pm.flush_all()
    print("所有页已刷新到磁盘")


if __name__ == "__main__":
    main()
