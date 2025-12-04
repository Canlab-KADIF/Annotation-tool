<template>
  <div :class="`${prefixCls}`">
    <CreateDatasetModal @register="registerCreateModal" />
    <div class="create-btn">
      <Button type="primary" :gradient="true" :size="ButtonSize.LG" @click="handleCreate">
        Create
      </Button>
    </div>

    <div class="wrapper">
      <div :class="`${prefixCls}-list`" ref="listPage">
        <div style="width: 260px; float: right"
          ><Input
            size="large"
            autocomplete="off"
            v-model:value="name"
            :placeholder="t('business.ontology.searchForm.searchItems')"
          >
            <template #suffix>
              <Icon icon="ant-design:search-outlined" style="color: #aaa" size="16" />
            </template> </Input
        ></div>
        <div v-show="list.length === 0" class="empty">
          <div class="text-center empty-wrapper">
            <img class="inline-block mb-4" width="136" :src="datasetEmpty" alt="" />
            <div class="mb-4 text-primary">No items in this dataset yet.</div>
            <!-- <Button :size="ButtonSize.LG" gradient @click="handleUpload">Upload Data</Button> -->
          </div>
        </div>
        <ScrollContainer
          :loadMore="loadMore"
          :canload="canload"
          v-show="list.length !== 0"
          ref="scrollRef"
          viewClass="dataset-list-card-scroll"
        >
          <ListCard
            class="listcard"
            v-for="item in list"
            :key="item.id"
            :data="item"
            @fetchList="fetchList"
            @closeCreateModal="closeCreateModal"
          />
        </ScrollContainer>
      </div>
      <div class="sider">
        <!-- <Input
          autocomplete="off"
          v-model:value="name"
          :placeholder="t('business.ontology.searchForm.searchItems')"
        >
          <template #suffix>
            <Icon icon="ant-design:search-outlined" style="color: #aaa" size="16" />
          </template>
        </Input> -->
        <div class="custom-item">
          <div class="font-bold custom-label">Sort</div>
          <Select style="flex: 1" size="small" v-model:value="sortField">
            <Select.Option
              v-for="item in datasetListSortOption"
              :key="item.value"
              :value="item.value"
            >
              {{ item.label }}
            </Select.Option>
          </Select>
        </div>
        <div class="custom-item">
          <Radio.Group v-model:value="sortType">
            <Radio v-for="item in SortTypeOption" :key="item.value" :value="item.value">
              <span class="radioText">{{ item.label }}</span>
            </Radio>
          </Radio.Group>
        </div>
        <div class="filter">
          <div class="flex justify-between mb-2 font-bold">
            <div>Filter</div>
            <div>
              <SvgIcon @click="resetFilter" name="reload" />
            </div>
          </div>
          <CollContainer icon="mdi:calendar-month" title="Created date">
            <DatePicker v-model:start="start" v-model:end="end" />
          </CollContainer>
          <CollContainer icon="ic:sharp-abc" title="Type">
            <CustomRadio :options="typeFilter" v-model:type="type" />
          </CollContainer>
        </div>
      </div>
    </div>
  </div>
</template>
<script lang="ts" setup>
  import { ref, provide, reactive, watch, onMounted, unref, watchEffect } from 'vue';
  import { useI18n } from '/@/hooks/web/useI18n';
  import { useDesign } from '/@/hooks/web/useDesign';
  import { Button, ButtonSize } from '/@@/Button';
  import { Input, Select, Radio } from 'ant-design-vue';
  import { datasetListApi, datasetListByNamesApi } from '/@/api/business/dataset';
  import {
    DatasetListItem,
    DatasetListGetResultModel,
    SortFieldEnum,
    datasetTypeEnum,
  } from '/@/api/business/model/datasetModel';
  import { useModal } from '/@/components/Modal';
  import CreateDatasetModal from './components/CreateDatasetForm.vue';
  import ListCard from './components/DatasetListCard.vue';
  import { datasetListSortOption, SortTypeOption } from '../datasetContent/components/data';
  import { SortTypeEnum } from '/@/api/model/baseModel';
  import { ScrollContainer, ScrollActionType } from '/@/components/Container/index';
  import { handleScroll } from '/@/utils/business/scrollListener';
  import CollContainer from '/@@/CollContainer/index.vue';
  import DatePicker from '/@@/DatePicker/index.vue';
  import type { Dayjs } from 'dayjs';
  import datasetEmpty from '/@/assets/images/dataset/dataset_empty.png';
  import { setEndTime, setStartTime } from '/@/utils/business/timeFormater';
  import CustomRadio from '/@@/CustomRadio/index.vue';
  import { useLoading } from '/@/components/Loading';
  import Icon, { SvgIcon } from '/@/components/Icon';
  import { useFlowLayout } from '/@/hooks/web/useFlowLayout';
  import { useRoute } from 'vue-router';

  const route = useRoute();
  const annotationStatus = ref<any>();
  const start = ref<Nullable<Dayjs>>(null);
  const type = ref<Nullable<string>>(null);
  const end = ref<Nullable<Dayjs>>(null);
  const scrollRef = ref<Nullable<ScrollActionType>>(null);
  const name = ref<string>('');
  // const sortField = ref<SortFieldEnum>(SortFieldEnum.CREATED_AT);
  const sortField = ref<SortFieldEnum>(SortFieldEnum.NAME);
  const sortType = ref<SortTypeEnum>(SortTypeEnum.ASC);
  const { t } = useI18n();
  const canload = ref(true);
  const { prefixCls } = useDesign('datasetList');
  const [registerCreateModal, { openModal: openCreateModal, closeModal: closeCreateModal }] =
    useModal();
  const total = ref<number>(0);
  const pageNo = ref<number>(1);
  const listPage = ref();

  // Parse dataset names from URL query parameter
  const filterDatasetNames = ref<string[]>([]);
  if (route.query.datasetNames) {
    const namesParam = Array.isArray(route.query.datasetNames) 
      ? route.query.datasetNames[0] 
      : route.query.datasetNames;
    filterDatasetNames.value = namesParam.split(',').map(name => name.trim()).filter(Boolean);
    console.log('Filtering datasets by names:', filterDatasetNames.value);
  }

  const typeFilter = [
    { label: 'Lidar Fusion', value: datasetTypeEnum.LIDAR_FUSION },
    { label: 'Lidar Basic', value: datasetTypeEnum.LIDAR_BASIC },
    { label: 'Image', value: datasetTypeEnum.IMAGE },
    { label: 'Text', value: datasetTypeEnum.TEXT },
  ];
  const [openFullLoading, closeFullLoading] = useLoading({
    target: listPage,
  });
  let { cardHeight, cardWidth, paddingX } = useFlowLayout('basic-datasetList-list', 105);
  watchEffect(() => {
    cardHeight, cardWidth, paddingX;
  });
  const loadMore = () => {
    if (canload.value) {
      pageNo.value++;
      fetchList(filterForm, true);
    }
  };
  onMounted(() => {
    setTimeout(() => {
      fetchList(filterForm);
    }, 50);
    handleScroll(
      scrollRef,
      () => {
        loadMore();
      },
      canload.value,
    );
  });
  let filterForm = reactive({
    name,
    sortField,
    createStartTime: start,
    createEndTime: end,
    ascOrDesc: sortType,
    type,
    annotationStatus: annotationStatus,
  });
  let timeout;
  watch(filterForm, (count) => {
    /* ... */

    if (
      (count.createStartTime && !count.createEndTime) ||
      (!count.createStartTime && count.createEndTime)
    ) {
      return;
    }
    clearTimeout(timeout);
    timeout = setTimeout(() => {
      handleReset();
      fetchList(count);
    }, 400);
  });
  const list = ref<DatasetListItem[]>([]);

  const fetchList = async (filter, fetchType?) => {
    openFullLoading();
    const params = {
      pageNo: pageNo.value,
      pageSize: 16,
      ...filter,
      createStartTime:
        filter.createStartTime && filter.createEndTime
          ? setStartTime(filter.createStartTime)
          : undefined,
      createEndTime:
        filter.createEndTime && filter.createStartTime
          ? setEndTime(filter.createEndTime)
          : undefined,
    };
    try {
      // Check if we should filter by dataset names (Server-side filtering)
      if (filterDatasetNames.value.length > 0) {
        // Use the new backend API for filtering
        const names = filterDatasetNames.value.join(',');
        const res = await datasetListByNamesApi(names);
        
        if (fetchType) {
          list.value = list.value.concat(res);
        } else {
          list.value = res;
        }
        
        // Since this API returns a list directly, we calculate total from list length
        total.value = res.length;
        
        // Disable load more since we got all filtered results at once
        canload.value = false;
      } else {
        // Default behavior: fetch all datasets with pagination
        if (fetchType) {
          const res: DatasetListGetResultModel = await datasetListApi(params);
          list.value = list.value.concat(res.list);
          if (res.list.length === 0) {
            canload.value = false;
          }
          total.value = res.total;
        } else {
          const res: DatasetListGetResultModel = await datasetListApi(params);
          list.value = res.list;
          total.value = res.total;
        }
      }
    } catch (e) {
      console.error('Error fetching dataset list:', e);
    }
    closeFullLoading();
  };

  // onBeforeMount(async () => {
  // });

  const handleCreate = () => {
    openCreateModal();
  };

  const handleReset = async () => {
    unref(scrollRef)?.scrollTo(0);
    pageNo.value = 1;
    canload.value = true;
  };

  const resetFilter = () => {
    filterForm.name = '';
    // filterForm.sortField = SortFieldEnum.CREATED_AT;
    filterForm.sortField = SortFieldEnum.NAME;
    filterForm.createStartTime = null;
    filterForm.createEndTime = null;
    filterForm.ascOrDesc = SortTypeEnum.ASC;
    filterForm.type = null;
    filterForm.annotationStatus = undefined;
  };

  provide('fetchList', () => {
    handleReset();
    fetchList(filterForm);
  });
</script>
<style lang="less" scoped>
  @wrapper-height: 40px;
  @prefix-cls: ~'@{namespace}-datasetList';
  .@{prefix-cls} {
    // display: flex;

    .empty {
      display: flex;
      justify-content: center;
      align-items: center;
      height: calc(100% - 60px);
    }

    .create-btn {
      position: absolute;
      top: -48px;
      z-index: 588;
      right: 20px;
    }

    .sider {
      width: 220px;
      padding: 10px;
      background: #fff;
      border-left: 1px solid #ccc;
      height: calc(100vh - @header-height);

      .custom-item {
        margin: 10px 0;
        display: flex;
        align-items: center;

        .custom-label {
          font-size: 16px;
          margin-right: 19px;
        }
      }

      .radioText {
        font-size: 12px;
        margin-left: -4px;
      }
    }

    .wrapper {
      width: 100%;
      display: flex;
    }
    &-list {
      flex: 1;
      width: 80%;
      padding: 20px 10px 0;
      height: calc(100vh - @header-height - @wrapper-height);

      :deep(.dataset-list-card-scroll) {
        display: grid;
        grid-template-columns: repeat(auto-fill, v-bind(cardWidth));
        // gap: 10px;
        justify-content: center;

        .listcard {
          margin: 0;
          padding: v-bind(paddingX);
          height: v-bind(cardHeight);
        }
      }
    }
  }
</style>
