# 思路

1. 插入。先插入到 mysql，然后插入到 redis。
2. 删除。先从 mysql 中删除，然后从 redis 中删除。
3. 更新。先更新 mysql，然后从 redis 中删除。
4. 查询。先从 redis 中读取，如果出错，则从 mysql 中读取。如果没有出错，但 redis 中不存在，则从 mysql 中读取，如果 mysql 中存在，则 redis 中保存一份。