/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.facebook.presto.operator;

import com.facebook.presto.spi.Page;
import com.facebook.presto.spi.PageBuilder;
import com.facebook.presto.spi.type.Type;
import com.google.common.collect.ImmutableList;

import java.util.Comparator;
import java.util.Iterator;
import java.util.List;
import java.util.NoSuchElementException;

import static java.util.Objects.requireNonNull;
import static java.util.stream.Collectors.toList;

public class MergeSort
{
    private final List<Type> groupByTypes;
    private final List<Type> types;

    public MergeSort(List<Type> groupByTypes, List<Type> types)
    {
        this.groupByTypes = requireNonNull(groupByTypes, "groupByTypes is null");
        this.types = requireNonNull(types, "types is null");
    }

    public Iterator<Page> merge(List<Iterator<Page>> channels)
    {
        List<PageIterator> channelIterators = channels.stream().map(ChannelIterator::new).collect(toList());

        while (channelIterators.size() > 1) {
            ImmutableList.Builder<PageIterator> builder = ImmutableList.builder();
            for (int i = 0; i < channelIterators.size(); i += 2) {
                PageIterator left = channelIterators.get(i);
                PageIterator right = new EmptyPageIterator();

                if (i + 1 < channelIterators.size()) {
                    right = channelIterators.get(i + 1);
                }
                builder.add(new BinaryMergeIterator(new PagePositionComparator(groupByTypes), left, right));
            }
            channelIterators = builder.build();
        }

        return new PageRewriteIterator(groupByTypes, types, channelIterators.get(0));
    }

    public static class PagePosition
    {
        private final Page page;
        private final int position;

        public PagePosition(Page page, int position)
        {
            this.page = requireNonNull(page, "page is null");
            this.position = requireNonNull(position, "position is null");
        }

        public Page getPage()
        {
            return page;
        }

        public int getPosition()
        {
            return position;
        }

        public boolean isEmpty()
        {
            return !(position < page.getPositionCount());
        }
    }

    public static interface PageIterator extends Iterator<PagePosition>
    {
    }

    public static class BinaryMergeIterator implements PageIterator
    {
        private final PageIterator left;
        private final PageIterator right;
        private final Comparator<PagePosition> comparator;
        private PagePosition currentLeft;
        private PagePosition currentRight;

        public BinaryMergeIterator(Comparator<PagePosition> comparator, PageIterator left, PageIterator right)
        {
            this.comparator = requireNonNull(comparator, "comparator is null");
            this.left = requireNonNull(left, "left is null");
            this.right = requireNonNull(right, "right is null");

            advanceLeft();
            advanceRight();
        }

        @Override
        public boolean hasNext()
        {
            return currentLeft != null || currentRight != null;
        }

        @Override
        public PagePosition next()
        {
            if (!hasNext()) {
                throw new NoSuchElementException();
            }

            boolean isLeft;

            if (currentLeft == null) {
                isLeft = false;
            }
            else if (currentRight == null) {
                isLeft = true;
            }
            else {
                isLeft = comparator.compare(currentLeft, currentRight) <= 0;
            }

            PagePosition result;
            if (isLeft) {
                result = currentLeft;
                advanceLeft();
            }
            else {
                result = currentRight;
                advanceRight();
            }
            return result;
        }

        private void advanceRight()
        {
            currentRight = null;
            while (currentRight == null && right.hasNext()) {
                currentRight = right.next();
                if (currentRight.isEmpty()) {
                    currentRight = null;
                }
            }
        }

        private void advanceLeft()
        {
            currentLeft = null;
            while (currentLeft == null && left.hasNext()) {
                currentLeft = left.next();
                if (currentLeft.isEmpty()) {
                    currentLeft = null;
                }
            }
        }
    }

    public static class ChannelIterator implements PageIterator
    {
        private final Iterator<Page> channel;
        private PagePosition current;

        public ChannelIterator(Iterator<Page> channel)
        {
            this.channel = requireNonNull(channel, "channel is null");
        }

        @Override
        public boolean hasNext()
        {
            return channel.hasNext() || (current != null && current.getPosition() + 1 < current.getPage().getPositionCount());
        }

        @Override
        public PagePosition next()
        {
            if (current == null || current.getPosition() + 1 >= current.getPage().getPositionCount()) {
                current = new PagePosition(channel.next(), 0);
            }
            else {
                current = new PagePosition(current.getPage(), current.getPosition() + 1);
            }
            return current;
        }
    }

    public static class PageRewriteIterator
            implements Iterator<Page>
    {
        private final List<Type> types;
        private final PageIterator pageIterator;
        private final List<Type> compareTypes;
        private final PageBuilder builder;
        private PagePosition currentPage = null;

        public PageRewriteIterator(List<Type> compareTypes, List<Type> types, PageIterator pageIterator)
        {
            this.compareTypes = compareTypes;
            this.types = types;
            this.pageIterator = pageIterator;
            this.builder = new PageBuilder(types);
        }

        @Override
        public boolean hasNext()
        {
            return currentPage != null || pageIterator.hasNext();
        }

        @Override
        public Page next()
        {
            builder.reset();

            if (currentPage == null) {
                currentPage = pageIterator.next();
            }

            PagePosition previousPage = currentPage;

            while (comparePages(currentPage, previousPage) == 0 || !builder.isFull()) {
                builder.declarePosition();
                for (int column = 0; column < types.size(); column++) {
                    Type type = types.get(column);
                    type.appendTo(currentPage.getPage().getBlock(column), currentPage.getPosition(), builder.getBlockBuilder(column));
                }
                previousPage = currentPage;

                if (pageIterator.hasNext()) {
                    currentPage = pageIterator.next();
                }
                else {
                    currentPage = null;
                    break;
                }
            }
            return builder.build();
        }

        private int comparePages(PagePosition left, PagePosition right)
        {
            for (int column = 0; column < compareTypes.size(); column++) {
                int compare = compareTypes.get(column).compareTo(
                        left.getPage().getBlock(column),
                        left.getPosition(),
                        right.getPage().getBlock(column),
                        right.getPosition());
                if (compare != 0) {
                    return compare;
                }
            }
            return 0;
        }
    }

    public static class PagePositionComparator
            implements Comparator<PagePosition>
    {
        private final List<Type> types;

        public PagePositionComparator(List<Type> types)
        {
            this.types = requireNonNull(types, "groupByTypes is null");
        }

        @Override
        public int compare(PagePosition left, PagePosition right)
        {
            for (int column = 0; column < types.size(); column++) {
                int compare = types.get(column).compareTo(left.getPage().getBlock(column), left.getPosition(),
                        right.getPage().getBlock(column), right.getPosition());
                if (compare != 0) {
                    return compare;
                }
            }
            return 0;
        }
    }

    public static class EmptyPageIterator implements PageIterator
    {
        @Override
        public boolean hasNext()
        {
            return false;
        }

        @Override
        public PagePosition next()
        {
            throw new NoSuchElementException();
        }
    }
}
