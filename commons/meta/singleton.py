def multiton(cls):
    instances = {}

    def get_instance(key):
        if key not in instances:
            instances[key] = cls(key)
        return instances[key]

    return get_instance
