from Tug.Util.TryQueue.Item import Item

from threading import Lock
from threading import Condition
import time

class TryQueue:

    def __init__(self):
        self.__first_item = None
        self.__last_item = None
        self.__lock = Condition(Lock())


    def add(self, value):
        # Acquire lock
        with self.__lock:
            # Create the item
            item = Item(value)

            # Do we have a last item?
            if(self.__last_item != None):
                # Yes, the new item now proceeds it
                item.proceeds = self.__last_item

                # The item this item proceeds, preceeds this item
                item.proceeds.preceeds = item
        
            # This item is now the last item
            self.__last_item = item

            # Do we have a first item?
            if(self.__first_item == None):
                # No, this is now our first item
                self.__first_item = item

            # Notify all waiting threads
            self.__lock.notify_all()


    def __pluck_item(self, item: Item):
        if (item.proceeds != None):
            item.proceeds.preceeds = item.preceeds

        if(item.preceeds != None):
            item.preceeds.proceeds = item.proceeds

        if(item == self.__first_item):
            self.__first_item = item.preceeds

        if(item == self.__last_item):
            self.__last_item = item.proceeds

    
    def get(self, where, wait = True):
        # Acquire lock
        with self.__lock:
            # Get the first item
            item = self.__first_item

            # Find the first item that satisfies the filter
            while item != None:
                # Does the item satisfy the filter?
                if(where(item.value)):
                    # Yes, pluck and return
                    self.__pluck_item(item)
                    return item.value

                # Get the next item
                item = item.preceeds

            # Should we wait?
            if(wait):
                # Nothing found, wait until an item gets added and test that
                self.__lock.wait_for(lambda: where(self.__last_item.value))

                # Return the value
                return self.__last_item.value

            return None

            

        

            