"""
缓存管理器
实现页缓存机制，支持LRU替换策略
提供缓存命中统计与替换日志输出功能
"""
from typing import Dict, Optional, List
from collections import OrderedDict
from dataclasses import dataclass
from enum import Enum
import time
from storage.page_manager import Page, PageManager
from utils.logger import logger

class ReplacementPolicy(Enum):
    """替换策略"""
    LRU = "LRU"  # 最近最少使用
    FIFO = "FIFO"  # 先进先出
    LRFU = "LRFU"  # 最近最少使用，最少访问


@dataclass
class CacheEntry:
    """缓存条目"""
    page: Page
    access_time: float
    access_count: int
    is_dirty: bool = False
    score: float = 0.0


@dataclass
class CacheStats:
    """缓存统计信息"""
    hits: int = 0
    misses: int = 0
    evictions: int = 0
    total_requests: int = 0
    
    @property
    def hit_rate(self) -> float:
        """缓存命中率"""
        if self.total_requests == 0:
            return 0.0
        return self.hits / self.total_requests
    
    @property
    def miss_rate(self) -> float:
        """缓存未命中率"""
        return 1.0 - self.hit_rate


class CacheManager:
    """缓存管理器"""
    
    def __init__(self, page_manager: PageManager, max_size: int = 100, 
                 policy: ReplacementPolicy = ReplacementPolicy.LRU,
                 decay: float = 0.5):
        self.page_manager = page_manager
        self.max_size = max_size
        self.policy = policy
        self.decay = decay
        self.cache: OrderedDict[int, CacheEntry] = OrderedDict()
        self.stats = CacheStats()
        self.eviction_log: List[Dict] = []
    
    def get_page(self, page_id: int) -> Optional[Page]:
        """获取页，优先从缓存中获取"""
        self.stats.total_requests += 1
        
        if page_id in self.cache:
            # 缓存命中
            self.stats.hits += 1
            entry = self.cache[page_id]
            entry.access_time = time.time()
            entry.access_count += 1

            if self.policy == ReplacementPolicy.LRFU:
                entry.score = self.decay * entry.score + 1
            
            # 更新LRU顺序
            if self.policy == ReplacementPolicy.LRU:
                self.cache.move_to_end(page_id)
            
            logger.log_cache_operation("读取",page_id=page_id,hit=True)
            return entry.page
        else:
            # 缓存未命中
            self.stats.misses += 1
            
            # 从存储管理器读取页
            page = self.page_manager.read_page(page_id)
            if page is None:
                return None
            
            # 添加到缓存
            self._add_to_cache(page_id, page)
            
            return page
    
    def put_page(self, page_id: int, page: Page, is_dirty: bool = False):
        """将页放入缓存"""
        if page_id in self.cache:
            # 更新现有条目
            entry = self.cache[page_id]
            entry.page = page
            entry.is_dirty = is_dirty
            entry.access_time = time.time()
            entry.access_count += 1
            
            if self.policy == ReplacementPolicy.LRU:
                self.cache.move_to_end(page_id)
        else:
            # 添加新条目
            self._add_to_cache(page_id, page, is_dirty)
    
    def _add_to_cache(self, page_id: int, page: Page, is_dirty: bool = False):
        """添加页到缓存"""
        # 检查缓存是否已满
        if len(self.cache) >= self.max_size:
            self._evict_page()
        
        # 添加新条目
        entry = CacheEntry(
            page=page,
            access_time=time.time(),
            access_count=1,
            is_dirty=is_dirty,
            score=1.0
        )
        
        self.cache[page_id] = entry
        
        # 对于FIFO策略，新条目添加到末尾
        if self.policy == ReplacementPolicy.FIFO:
            self.cache.move_to_end(page_id)

        logger.log_cache_operation("写入", page_id=page_id, hit=False)
    def _evict_page(self):
        """驱逐页"""
        if not self.cache:
            return
        
        # 选择要驱逐的页
        if self.policy == ReplacementPolicy.LRU:
            # LRU: 移除最久未使用的页
            page_id, entry = self.cache.popitem(last=False)
        elif self.policy == ReplacementPolicy.LRFU:
            # 驱逐分数最低的
            page_id = min(self.cache, key=lambda k: self.cache[k].score)
            entry = self.cache.pop(page_id)
        else:  # FIFO
            # FIFO: 移除最早进入的页
            page_id, entry = self.cache.popitem(last=False)
        
        # 如果页是脏的，写回存储
        if entry.is_dirty:
            self.page_manager.write_page(page_id, entry.page)
        
        # 记录驱逐日志
        self.stats.evictions += 1
        self.eviction_log.append({
            'page_id': page_id,
            'access_count': entry.access_count,
            'last_access': entry.access_time,
            'score': entry.score,
            'is_dirty': entry.is_dirty,
            'timestamp': time.time()
        })
        logger.log_cache_operation("释放", page_id=page_id, hit=True)
    
    def flush_page(self, page_id: int) -> bool:
        """刷新指定页到存储"""
        if page_id not in self.cache:
            return False
        
        entry = self.cache[page_id]
        if entry.is_dirty:
            success = self.page_manager.write_page(page_id, entry.page)
            if success:
                entry.is_dirty = False
            return success
        
        return True
    
    def flush_all(self):
        """刷新所有脏页到存储"""
        for page_id, entry in self.cache.items():
            if entry.is_dirty:
                self.page_manager.write_page(page_id, entry.page)
                entry.is_dirty = False
    
    def mark_dirty(self, page_id: int):
        """标记页为脏"""
        if page_id in self.cache:
            self.cache[page_id].is_dirty = True
    
    def remove_page(self, page_id: int) -> bool:
        """从缓存中移除页"""
        if page_id in self.cache:
            entry = self.cache[page_id]
            
            # 如果是脏页，先写回
            if entry.is_dirty:
                self.page_manager.write_page(page_id, entry.page)
            
            del self.cache[page_id]
            return True
        
        return False
    
    def clear_cache(self):
        """清空缓存"""
        # 刷新所有脏页
        self.flush_all()
        
        # 清空缓存
        self.cache.clear()
    
    def get_cache_stats(self) -> CacheStats:
        """获取缓存统计信息"""
        return self.stats
    
    def get_cache_info(self) -> Dict:
        """获取缓存详细信息"""
        return {
            'max_size': self.max_size,
            'current_size': len(self.cache),
            'policy': self.policy.value,
            'stats': {
                'hits': self.stats.hits,
                'misses': self.stats.misses,
                'evictions': self.stats.evictions,
                'total_requests': self.stats.total_requests,
                'hit_rate': self.stats.hit_rate,
                'miss_rate': self.stats.miss_rate
            },
            'cached_pages': list(self.cache.keys())
        }
    
    def get_eviction_log(self, limit: int = 10) -> List[Dict]:
        """获取驱逐日志"""
        return self.eviction_log[-limit:]
    
    def print_stats(self):
        """打印缓存统计信息"""
        info = self.get_cache_info()
        stats = info['stats']
        
        print("缓存统计信息:")
        print(f"  最大容量: {info['max_size']}")
        print(f"  当前大小: {info['current_size']}")
        print(f"  替换策略: {info['policy']}")
        print(f"  总请求数: {stats['total_requests']}")
        print(f"  命中次数: {stats['hits']}")
        print(f"  未命中次数: {stats['misses']}")
        print(f"  驱逐次数: {stats['evictions']}")
        print(f"  命中率: {stats['hit_rate']:.2%}")
        print(f"  未命中率: {stats['miss_rate']:.2%}")
    
    def print_eviction_log(self, limit: int = 5):
        """打印驱逐日志"""
        log = self.get_eviction_log(limit)
        
        print(f"\n最近{len(log)}次驱逐记录:")
        for entry in log:
            print(f"  页{entry['page_id']}: 访问{entry['access_count']}次, "
                  f"最后访问{entry['last_access']:.2f}, "
                  f"脏页={entry['is_dirty']}")


def main():
    """测试缓存管理器"""
    from .page_manager import PageManager
    
    # 创建页管理器和缓存管理器
    pm = PageManager("test_cache.dat")
    cm = CacheManager(pm, max_size=3, policy=ReplacementPolicy.LRU)
    
    print("缓存管理器测试:")
    
    # 分配一些页
    page_ids = []
    for i in range(5):
        page_id = pm.allocate_page("data")
        page_ids.append(page_id)
        print(f"分配页 {page_id}")
    
    # 访问页，测试缓存
    print("\n访问页测试:")
    for page_id in page_ids:
        page = cm.get_page(page_id)
        if page:
            print(f"获取页 {page_id}: 成功")
        else:
            print(f"获取页 {page_id}: 失败")
    
    # 打印缓存统计
    cm.print_stats()
    
    # 再次访问一些页
    print("\n再次访问页测试:")
    for page_id in page_ids[:3]:
        page = cm.get_page(page_id)
        if page:
            print(f"获取页 {page_id}: 成功")
    
    # 打印统计和驱逐日志
    cm.print_stats()
    cm.print_eviction_log()
    
    # 清理
    cm.clear_cache()


if __name__ == "__main__":
    main()
