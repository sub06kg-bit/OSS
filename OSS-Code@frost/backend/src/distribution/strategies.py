"""Shard distribution strategies"""
from typing import List, Dict
from abc import ABC, abstractmethod


class DistributionStrategy(ABC):
    @property
    @abstractmethod
    def name(self) -> str:
        pass
    
    @abstractmethod
    def assign(self, num_shards: int, nodes: List[str]) -> Dict[int, str]:
        pass


class RoundRobinStrategy(DistributionStrategy):
    @property
    def name(self) -> str:
        return "round_robin"
    
    def assign(self, num_shards: int, nodes: List[str]) -> Dict[int, str]:
        return {i: nodes[i % len(nodes)] for i in range(num_shards)}


class ErasureCodingStrategy(DistributionStrategy):
    def __init__(self, k=6, m=3):
        self.k = k
        self.m = m
    
    @property
    def name(self) -> str:
        return f"erasure_coding_{self.k}+{self.m}"
    
    def assign(self, num_shards: int, nodes: List[str]) -> Dict[int, str]:
        return {i: nodes[i % len(nodes)] for i in range(num_shards)}

