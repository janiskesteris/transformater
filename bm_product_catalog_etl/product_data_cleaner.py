from pyarrow import array as pyarrow_array

class ProductDataCleaner:
    def filter(self, batch):
        mask = self.__valid_products_mask(batch)
        valid_batch = batch.filter(mask)
        invalid_batch = batch.filter(self.__invert_mask(mask))
        return valid_batch, invalid_batch

    def __valid_products_mask(self, batch):
        return pyarrow_array([bool(image) for image in batch.to_pydict()['image']])

    def __invert_mask(self, mask):
        return pyarrow_array(not i.as_py() for i in mask)