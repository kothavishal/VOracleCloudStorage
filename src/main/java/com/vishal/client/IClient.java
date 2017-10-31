package com.vishal.client;

import com.vishal.api.Stream;

public interface IClient {

   public void put(String key, Stream input);

   public Stream get(String key);

   public void deleteContainer(String name);
}
