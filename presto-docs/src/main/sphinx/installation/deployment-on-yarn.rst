======================================
Deploying Presto on Yarn-based cluster
======================================

Presto can be installed on Yarn-based cluster. Presto integration with
yarn is provided by using `Apache Slider`_. To install presto on yarn cluster
first you need to wrap presto distribution binary with Slider package. You can do that by: 

.. parsed-literal::

  $ git clone git@github.com:prestodb/presto-yarn.git
  $ cd prest-yarn
  $ mvn install -Dpresto.version=\ |version|\

After that you will find a Slider package under: ``presto-yarn-package/target/presto-yarn-package-*.zip``. Once you have a package you can install presto via `Ambari`_ or `manually`_.

  .. _Apache slider: https://slider.incubator.apache.org/
  .. _manually: https://github.com/prestodb/presto-yarn/blob/master/README.md#manual-installation-via-slider
  .. _Ambari: https://github.com/prestodb/presto-yarn/blob/master/README.md#-installation-using-ambari-slider-view
