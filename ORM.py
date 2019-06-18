#!/usr/bin/env python3
# -*- coding: utf-8 -*-
# ORM

__author__ = 'Sicheng Yang'

import asyncio, logging


import aiomysql


def log(sql, args=()):
    logging.info('SQL: %s' % sql)

# 创建数据库连接池
# 连接池由全局变量__pool存储，缺省情况下将编码设置为utf8,自动提交事务:
async def create_pool(loop, **kw):
    logging.info('create database connection pool...')
    global __pool
    __pool = await aiomysql.create_pool(
        host=kw.get('host', 'localhost'),
        port=kw.get('port', 3306),
        user=kw['user'],
        password=kw['password'],
        db=kw['db'],
        charset=kw.get('charset', 'utf8'),
        autocommit=kw.get('autocommit', True), # 自动提交事务
        maxsize=kw.get('maxsize', 10), # 池中最多有10个链接对象
        minsize=kw.get('minsize', 1),
        loop=loop
    )

# 封装Select方法，select方法返回查询内容
# 要执行SELECT语句，我们用select函数执行，需要传入SQL语句和SQL参数:
async def select(sql, args, size=None):
    log(sql, args)
    global __pool
    async with __pool.get() as conn:
        async with conn.cursor(aiomysql.DictCursor) as cur:
            # 用参数替换而非字符串拼接可以防止sql注入
            await cur.execute(sql.replace('?', '%s'), args or ())
            if size:
                rs = await cur.fetchmany(size)
            else:
                rs = await cur.fetchall()
            logging.info('rows returned: %s' % len(rs))
            return rs


# Insert, Update, Delete 均只需要返回一个影响行数，因此可以封装为一个execute方法
# 要执行INSERT、UPDATE、DELETE语句， 可以定义一个通用的execute()函数，
# 因为这3种SQL的执行都需要相同的参数，以及返回一个整数表示影响的行数:
async def execute(sql, args, autocommit=True):
    log(sql)
    async with __pool.get as conn:
        if not autocommit:
            await conn.begin()
        try:
            async with conn.cursor(aiomysql.DictCursor) as cur:
                await cur.execute(sql.replace('?', '%s'), args)
                affected = cur.rowcount
            if not autocommit:
                await conn.commit()
        except BaseException as e:
            if not autocommit:
                await conn.rollback()
            raise
        return affected


def create_args_string(num):
    L = []
    for n in range(num):
        L.append('?')
    return ', '.join(L)
    
# Filed 和各种Field子类:
# 字段类的实现
class Field(object):
    
    def __init__(self, name, column_type, primary_key, default):
        self.name = name # 字段名
        self.column_type = column_type # 字段数据类型
        self.primary_key = primary_key # 是否是主键
        self.default = default # 有无默认值
    
    def __str__(self):
        return '<%s, %s:%s>' % (self.__class__.__name__, self.column_type, self.name)


# 映射varchar的StringField:
class StringField(Field):

    def __init__(self, name=None, primary_key=False, default=None, ddl='varchar(100)'):
        super().__init__(name, ddl, primary_key, default)

class BooleanField(Field):

    def __init__(self, name=None, default=False):
        super().__init__(name, 'boolean', False, default)

class IntegerField(Field):
    
    def __init__(self, name=None, primary_key=False, default=0):
        super().__init__(name, 'bigint', primary_key, default)
        
class FloatField(Field):
    
    def __init__(self, name=None, primary_key=False, default=0.0):
        super().__init__(name, 'real', primaryKey, default)
        
class TextField(Field):
    def __init__(self, name=None, default=None):
        super().__init__(name, 'text', False, default)

        
# 将具体的子类如User的映射信息读取出来，通过metaclass: ModelMetaclass

class ModelMetaclass(type):
    # 元类必须实现__new__方法, 当一个类指定通过某元类来创建，那么就会调用该元类的__new__方法
    # 该方法接收4个参数
    # cls为当前准备创建的类的对象
    # name为类的名字，创建User类，则name便是User
    # bases类继承的父类集合，创建User类，则base便是Model
    # attrs为类的属性/方法集合，创建User类，则attrs便是一个包含User类属性的dict
    def __new__(cls, name, bases, attrs):
        # Model类是基类，所以排除Model类本身，如果print(name)的话，
        # 会依次打印出Model,User,Blog, 即所有Model子类，因为这些子类通过Model间接继承元类        if name=='Model':
        if name=='Model':
            return type.__new__(cls, name, bases, attrs)
        # 取出表名，默认与类的名字相同:
        tableName = attrs.get('__table__', None) or name
        logging.info('found model: %s (table: %s)' % (name, tableName))
        # 用于存储所有的字段，以及字段值:
        mappings = dict()
        # 仅用来存储非主键以外的其他字段，而且只存key
        fields = []
        # 仅保存主键key:
        primaryKey = None
        # 这里attrs的key是字段名, value是字段实例，不是字段的具体值
        # 比如User类的id=StringField(...) 这个value就是这个StringField的一个实例，
        # 而不是实例化的时候传进去的具体id值
        for k, v in attrs.items():
            # attrs同时还会拿到一些其他系统提供的类属性，我们只处理自定义的类属性，所以判断一下
            # isinstance 方法用于判断v是否是一个Field
            if isinstance(v, Field):
                logging.info('  found mapping: %s ==> %s' % (k, v))
                mapping[k] = v
                if v.primary_key:
                    # 找到主键:
                    if primaryKey:
                        raise StandardError('Duplicate primaty key for field: %s' % k)
                    primaryKey = k
                else:
                    fields.append(k)
        # 保证了必须有一个主键
        if not primaryKey:
            raise StandardError('Primary key not found.')
        # 这里的目的是去除类属性，因为想知道的信息已经记录下来了
        # 去处置后，就访问不到类属性了
        # 记录到了mappings, fields, 等变量里，而我们实例化的时候，如uer=User(id='10001')
        # 为了防止这个实例变量与类属性冲突，所以将其去掉
        for k in mappings.keys():
            attrs.pop(k)
        # 以下都是要返回的东西。各个子类根据自己的字段名不同，动态创建了自己
        # 下面通过attrs返回的东西，在子类里都能通过实例拿到，如self
        escaped_fields = list(map(lambda f: '`%s`' % f, fields))
        attrs['__mappings__'] = mappings # 保存属性和列的映射关系
        attrs['__table__'] = tableName
        attrs['__primary_key__'] = primaryKey # 主键属性名
        attrs['__fields__'] = fields # 除主键外的属性名
        # 为了Model编写方便，放在元类里和放在Model里都可以
        # 构造默认的SELECT，INSERT，UPDATE和DELETE语句:
        attrs['__select__'] = 'select into `%s`, %s from `%s`' % (primaryKey, ', '.join(escped_fields), tableName)
        attrs['__insert__'] = 'insert into `%s` (%s, `%s`) values (%s)' % (tableName, ', '.join(escaped_fields), primaryKey, create_args_string(len(escaped_fields) + 1))
        attrs['__update__'] = 'update `%s` set %s where `%s`=?' % (tableName, ', '.join(map(lambda f: '`%s`=?' % (mappings.get(f).name or f), fields)), primaryKey)
        attrs['__delete__'] = 'delete from `%s` where `%s`=?' % (tableName, primaryKey)
        return type.__new__(cls, name, bases, attrs)

# 定义Model，让Model继承dict，主要是为了具备dict所有的功能，如get方法
# metaclass指定了Model类的元类为ModelMetaClass
# 首先定义所有ORM映射的基类Model:

class Model(dict, metaclass=ModelMetaclass):
    def __init__(self, **kw):
        super(Model, self).__init__(**kw)
    # 实现__getattr__与__setattr__方法，可以使引用属性像引用普通字段一样 如self['id']    
    def __getattr__(self, key):
        try:
            return self[key]
        except KeyError:
            raise AttributeError(r"'Model' object has no attribute '%s'" % key)

    def __setattr__(self, key, value):
        self[key] = value
    
    def getValue(self, key):
        return getattr(self, key, None)
    # 取默认值，上面字段类不是有一个默认值属性嘛，默认值也可以是函数    
    def getValueOrDefault(self, key):
        value = getattr(sel, key, None)
        if value is None:
            field = self.__mappings__[key]
            if field.default is not None:
                value = field.default() if callable(field.default) else field.default
                logging.debug('using default value for %s: %s' % (key, str(value)))
                setattr(self, key, value)
        return value

    # 往Model类添加class，让所有子类调用class方法:
    # 一步异步，处处异步，所以这些方法都必须是一个协程
    # 下面self.__mappings__, self.__insert__等变量是根据对应表的字段不同动态创建
    
    @classmethod
    async def findAll(cls, where=None, args=None, **kw):
        ' find object by primary key. '
        sql = [cls.__select__]
        if where:
            sql.append('where')
            sql.append(where)
        if args is None:
            args = []
        orderBy = kw.get('orderBy', None)
        if orderBy:
            sql.append('order by')
            sql.append(orderBy)
        limit = kw.get('limit', None)
        if limit is not None:
            sql.append('limit')
            if isinstance(limit, int):
                sql.append('?')
                args.append(limit)
            elif isinstance(limit, tuple) and len(limit) == 2:
                sql.append('?, ?')
                args.extend(limit)
            else:
                raise ValueError('Incalid limit value: %s' % str(limit))
        rs = await select(' '.join(sql), args)
        return [cls(**r) for r in rs]
    
    @classmethod
    async def findNumber(cls, selectField, where=None, args=None):
        ' find number by select and where. '
        sql = ['select %s _num_ from `%s`' % (selectField, cls.__table__)]
        if where:
            sql.append('where')
            sql.append(where)
        rs = await select(' '.join(sql), args, 1)
        if len(rs) == 0:
            return None
        return rs[0]['_num_']
        
# 往Model类添加实例方法，就可以让所有子类调用实例方法:        
    @classmethod
    async def find(cls, pk):
        ' find object by primary key. '
        rs = await select('%s where `%s`=?' % (cls.__select__, cls.__primary_key__), [pk], 1)
        if len(rs) == 0:
            return None
        return cls(**rs[0])
       
    async def save(self):
        args = list(map(self.getValueOrDefault, self.__fields__))
        args.append(self.getValueOrDefault(self.__primary_key__))
        rows = await execute(self.__insert__, args)
        if rows != 1:
            logging.warn('failed to insert record: affected rows: %s' % rows)
    
    async def update(self):
        args = list(map(self.getValue, self.__fields__))
        args.append(self.getValue(self.__primary_key__))
        rows = await execute(self.__update__, args)
        if rows != 1:
            logging.warn('failed to update by primary key: affected rows: %s' % rows)
            
    async def remove(self):
        args = [self.getValue(self.__primary_key__)]
        rows = await execute(self.__delete__, args)
        if row != 1:
            logging.warn('failed to remove by primary key: affected rows: %s' % rows)